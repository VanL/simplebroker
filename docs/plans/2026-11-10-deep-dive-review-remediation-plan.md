# 2026-11-10 Deep-Dive Review Remediation Plan

Date: 2026-11-10
Status: completed
Class: Class 5+P (base Class 4 [DOM-5] risky cross-backend/concurrency/containment work + Class 5 normative `[SB-*]` spec clarifications + `+P` governance/CI gate hardening; effective Class 5 with `docs/agent-context/runbooks/hardening-plans.md` required)
Promotion strategy: Strategy A — Spec-Promotion Slice first (normative spec text + `## Related Plans` backlinks land before code slices; Final Traceability Slice adds `_Verification_` test citations after new tests exist so `tests/test_doc_gates.py` stays green at every slice)

## Goal

Remediate every empirically verified finding (`Findings 1–10g`) from the
2026-11-10 whole-repository critical deep-dive review of SimpleBroker (`v8.8.1`,
`HEAD` `93d2aec`) without eroding the conceptual boundaries in
`docs/program-theory.md` (`[THEORY-1]`, `[THEORY-2]`, `[THEORY-5]`,
`[THEORY-6]`, `[REV-THEORY-001]`, `[REV-THEORY-005]`, `[REV-THEORY-006]`) or
re-opening settled negative-knowledge decisions (`[ALT-THEORY-001]`,
`[ALT-IMPL09-001]`, `[ALT-IMPL09-002]`).

## Human-Review Decision Points

1. **`HR-1` — Conservative `sslpassword=` Redaction + Existing Fail-Closed Last-`@` Masking (`Finding 8` / External Review `F2` / Owner Scoping Review `2026-11-10`)**:
   - **Why human review was surfaced**: Carving a multi-clause RFC 3986 port/query
     exemption out of `_redact_raw_url_userinfo` to avoid over-masking display
     strings on passwordless `scheme://host:port/db?k=v@domain` URLs introduces
     a syntactic overlap with malformed unencoded URL passwords
     (`scheme://u:<port>/<seg>?...@host`). Because `redact_backend_target` and
     `_backend_target_has_password` never modify `BrokerTarget.target` (the live
     connection DSN) and affect only display strings and the `.broker.toml`
     advisory warning, owner scoping review (`2026-11-10`) selected the simpler
     fail-closed option: widen `_CONNINFO_PASSWORD_RE` to cover `sslpassword=`
     (`r"(?i)(\b(?:ssl)?password\s*=\s*)..."`), keep `_redact_raw_url_userinfo`
     masking conservatively through the last `@` with zero URI-exemption clauses,
     and suppress the CLI `.broker.toml` advisory under `--quiet`.

## Outcome

When this plan is complete:

1. Clean clones (`git clone --no-local`) and GitHub Actions CI (`fetch-depth: 0`
   on doc gates) pass `bin/coalesce-check`, `bin/check-doc-paths`,
   `bin/check-plan-context`, and `bin/check-dom15-fixtures` with zero dangling
   SHAs (rejecting local-only branch/tag refs unless `--allow-local-only` is
   passed), house-style sibling-plan citation support, and zero false-pass blind
   spots; `require_green_workflows.py` (`github_api_get`) tolerates transient
   GitHub REST API `429`/`5xx`/transport and truncated-body blips during release
   gating.
2. Same-thread process-session `Queue` / `BrokerSession` sharing preserves each
   nested and out-of-order caller's cooperative `_stop_event` via token-keyed
   per-lease frames instead of clobbering it with a newly allocated unset
   `threading.Event()` or popping a live sibling lease's frame (restoring ~`50 ms`
   cooperative stop under contention instead of stalling `30 s`).
3. SQL `BrokerCore` alias caching uses an optimistic seqlock (`v1 -> rows -> v2`)
   with monotonic `alias_version` increments (never returning internal `-1`
   sentinels from `get_alias_version()`), `_run_with_retry` wrapping, and
   rollback cache invalidation; `BrokerCore.insert_messages` publishes the
   in-transaction `meta.last_ts` floor without post-commit SQL queries; and
   `BrokerCore.status` reads `database_size_bytes` under `_lock` and
   `_run_with_retry`.
4. `BaseWatcher.run_in_thread` synchronizes `_thread` publication and duplicate
   active-run detection (`_lifecycle_state_locked()`, using `_WatcherThread`
   completion marking so immediate restart after an unhandled crash succeeds
   even inside `threading.excepthook`) under `_stop_lock` while preserving
   `[SB-API-6]` stop-before-start semantics, and removes the superseded legacy
   `BaseWatcher.__del__` method; low-level generator `batch_size` (`None`
   preserving validated positive `effective_config["GENERATOR_BATCH_SIZE"]` for
   `claim_generator`/`move_generator` per `[SB-API-4]` and `PEEK_BATCH_SIZE` for
   `peek_generator`, or explicit `>= 1` `int`) and re-entrancy diagnostics
   behave consistently across SQLite, PostgreSQL, and Valkey/Redis.
5. Destructive CLI `--cleanup` (landed first in Slice 2) enforces `[SB-CLI-2]`
   physical containment on ordinary relative legacy-SQLite targets (blocking
   `link.db -> ../outside/victim.db` while preserving missing-directory `exit 0`
   no-ops and in-root symlink namespace freezing); `_targets.py` redacts
   `sslpassword=` while preserving fail-closed last-`@` URL masking;
   `broker move -m ... --all --json` emits structured `INVALID_ARGUMENT` JSON;
   plain-text CLI stderr sanitizes terminal control/escape/surrogate characters;
   and `--quiet` suppresses owned stderr human commentary (`cmd_move` cap notice
   and CLI project-config inline-password advisory).
6. PostgreSQL's existing option validation owners (`require_schema_name` in
   `extensions/simplebroker_pg/simplebroker_pg/validation.py` and module-level
   `verify_env` in `extensions/simplebroker_pg/simplebroker_pg/plugin.py`)
   reject unknown `backend_options` keys with `DatabaseError` before opening any
   PostgreSQL connection (`[SB-API-2]`), preventing typoed `schema` keys from
   silently routing into the default shared schema while keeping `Queue(...)`
   construction lazy for non-Redis plugins (`test_backend_plugin_resolution.py`
   and core-only CI).
7. `simplebroker_redis` loops `VACUUM_CLAIMED` until claimed rows are drained
   (`int(deleted) == batch_size`, honoring `_stop_event`, `[SB-OPS-6]`); embeds
   post-loop best-effort `redis.pcall('PUBLISH', ...)` queue notifications (via
   `ARGV` for Redis/Valkey Cluster slot safety and channel-ACL tear immunity),
   `persisted_last_ts` return, and monotonic `HINCRBY meta alias_version 1`
   (`ADD_ALIAS`, `RENAME_QUEUE`, and new `REMOVE_ALIAS`) inside Lua scripts
   while removing post-commit `_publish`, dead `activity:all` / `_wildcard_version`,
   and dead `_alias_cache`; translates `redis.RedisError` via
   `_translate_redis_error(exc)` across every remaining `RedisBrokerCore`
   operation while preserving `TRANSPORT-AMBIGUOUS` non-replay (`[SB-API-3]`,
   `[SB-API-6]`); keys active generator batches per lease token with captured
   owner thread ID under `_lock` (so foreign-thread close/GC removes the lease
   without stranding the owner thread) and clears inherited state on fork; and
   closes parity gaps in `init_backend` / `validation.connect`.
8. Canonical specs, implementation docs, guides, `README.md`, `CONTRIBUTING.md`,
   `CHANGELOG.md`, and standalone `examples/` (`sqlite_connect.py` URI
   validation and `dead_letter_queue.sh` JSON object envelope checks) match live
   v8/v9 runtime behavior.

## Source Documents

- Theory & negative knowledge:
  - `docs/program-theory.md` (`[THEORY-1]`, `[THEORY-2]`, `[THEORY-5]`,
    `[THEORY-6]`, `[ALT-THEORY-001]`, `[REV-THEORY-001]`, `[REV-THEORY-005]`,
    `[REV-THEORY-006]`)
  - `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
    (`[ALT-IMPL09-001]`, `[ALT-IMPL09-002]`)
  - `docs/lessons.md`
  - `docs/coalescing.md`
- Process & governance specs / runbooks:
  - `AGENTS.md`
  - `docs/specs/01-development-documentation-operating-model.md` (`[DOM-2]`,
    `[DOM-5]`, `[DOM-10]`, `[DOM-10.1]`, `[DOM-10.2]`, `[DOM-11]`, `[DOM-15]`,
    `[DOM-16]`)
  - `docs/specs/product-section-registry.md`
  - `docs/agent-context/runbooks/writing-plans.md`
  - `docs/agent-context/runbooks/hardening-plans.md`
  - `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`
  - `docs/agent-context/runbooks/maintaining-traceability.md`
  - `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
  - `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- Canonical product specs touched or governing behavior:
  - `docs/specs/10-cli.md` (`[SB-CLI-1]`, `[SB-CLI-2]`, `[SB-CLI-3]`,
    `[SB-CLI-4]`)
  - `docs/specs/11-delivery.md` (`[SB-DELIVERY-5]`, `[SB-DELIVERY-6]`,
    `[SB-DELIVERY-9]`)
  - `docs/specs/13-message-identity.md` (`[SB-ID-1]`, `[SB-ID-2]`, `[SB-ID-3]`)
  - `docs/specs/16-python-library-api.md` (`[SB-API-1]`, `[SB-API-2]`,
    `[SB-API-3]`, `[SB-API-4]`, `[SB-API-5]`, `[SB-API-6]`, `[SB-API-9]`,
    `[SB-API-10]`, `[SB-API-11]`)
  - `docs/specs/17-ops.md` (`[SB-OPS-1]`, `[SB-OPS-6]`, `[SB-OPS-7]`)
- Implementation architecture docs & sibling plans:
  - `docs/implementation/03-agent-inventory.md`
  - `docs/implementation/04-cross-thread-finalization-poisoning.md`
  - `docs/implementation/06-process-session-core-ownership.md`
  - `docs/implementation/07-complexity-and-state-machine-map.md`
  - `docs/implementation/10-ruff-suppression-registry.md`
  - `docs/plans/2026-11-10-backend-thread-ownership-plan.md`
  - `docs/plans/2026-11-10-fork-inherited-activity-waiter-plan.md`

## Investigation Disposition Matrix

Per `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md` §5a, every
verified audit finding maps explicitly to its owning slice and disposition:

| Finding | Severity | Summary | Disposition | Owning Slice |
| :--- | :---: | :--- | :--- | :--- |
| **1** | HIGH | Dangling pre-rebase SHAs (`e04c27a`, `f78612e`) in `docs/coalescing.md` masked by local loose objects and shallow CI checkouts | **Remediate** | Slice 1 |
| **2** | HIGH | `RedisBrokerCore.vacuum()` runs `VACUUM_CLAIMED` once (`1,000` rows) instead of looping until drained (`[SB-OPS-6]`) | **Remediate** | Slice 3 |
| **3** | HIGH | `RedisBrokerCore` leaks raw `redis.RedisError` across ~18 methods and runs non-atomic post-commit `_publish()` + dead `activity:all` (`[SB-API-3]`, `[SB-API-6]`) | **Remediate** | Spec-Promotion + Slice 3 |
| **4** | HIGH | Same-thread `BrokerCore._stop_event` clobber across shared process-session `Queue` handles stalls cooperative shutdown for `30 s` | **Remediate** | Slice 2 |
| **5** | MED-HIGH | SQL `BrokerCore` alias cache permanent staleness (inverted autocommit read order), wall-clock `alias_version` in SQL and Redis (`ADD_ALIAS`, `RENAME_QUEUE`, `remove_alias`), missing SQL retry & rollback invalidation | **Remediate** | Slice 2 + Slice 3 |
| **6** | MED-HIGH | Destructive CLI `--cleanup` skips relative-target physical containment (`link.db -> ../outside/victim.db` deletes external DB) | **Remediate** (Owner decision Q1) | Spec-Promotion + Slice 2 |
| **7** | MED-HIGH | PostgreSQL option validators (`verify_env`, `require_schema_name`) silently ignore unknown `backend_options` (`schmea` typo routes into default shared schema; `[SB-API-2]`) | **Remediate** | Spec-Promotion + Slice 3 |
| **8** | MEDIUM | `_targets.py` misses conninfo `sslpassword=` in redaction and inline-password detection | **Remediate** (conservative `sslpassword=` + fail-closed last-`@` URL masking; owner-approved `2026-11-10`) | Spec-Promotion + Slice 2 |
| **9** | MEDIUM | False-pass bugs in `bin/coalesce-check`, `bin/check-doc-paths` (+ dangling cue in `docs/program-theory.md`), `bin/check-plan-context`, `bin/check-dom15-fixtures`, and unretried `require_green_workflows.py` (`github_api_get`) | **Remediate** | Slice 1 |
| **10a** | LOW-MED | Unlocked `self._thread` publication race & duplicate active-run window in `BaseWatcher.run_in_thread`, plus superseded legacy `BaseWatcher.__del__` | **Remediate** | Slice 2 |
| **10b** | LOW-MED | Thread-unsafe scalar `_active_generator_batch` / `_active_generator_batch_owner` on direct `RedisBrokerCore` and missing fork reset in `_check_fork_safety` | **Remediate** | Slice 3 |
| **10c** | LOW-MED | Post-commit unretried `refresh_last_timestamp()` in `BrokerCore.insert_messages` (and Redis `INSERT_MESSAGES`/`BROADCAST_MESSAGE`) + unlocked `database_size_bytes` in `BrokerCore.status` | **Remediate** (Owner decision Q2) | Slice 2 + Slice 3 |
| **10d** | LOW | Re-entrancy `RuntimeError` recommends `"a separate BrokerDB/Queue instance"`, which still shares the thread-local session `BrokerCore` when persistent | **Remediate** | Slice 2 + Slice 3 |
| **10e** | LOW-MED | Redis `init_backend` (IPv6 brackets, `BACKEND_USER`, supplemental `BACKEND_PASSWORD`, redacted URL validation), `validation.connect` leak on `ping()` failure, and explicit invalid generator `batch_size <= 0` divergence | **Remediate** | Slice 2 + Slice 3 |
| **10f** | LOW | `broker move -m ... --all --json` pre-JSON `argparse` error, unsanitized control/ANSI/surrogate chars on plain stderr, and `--quiet` leaks (`cmd_move` cap + CLI inline-password commentary) | **Remediate** | Spec-Promotion + Slice 2 |
| **10g** | LOW | Drift across `16-python-library-api.md` (`v8` typo), `07-complexity-and-state-machine-map.md`, `backends.md`, `python.md`, `README.md`, `CONTRIBUTING.md`, `03-agent-inventory.md`, `_dump.py`, `examples/sqlite_connect.py` (`validate_database_path` URI), and `examples/dead_letter_queue.sh` (`jq` object envelope check + `printf`) | **Remediate** | Spec-Promotion + Slice 4 |

## No-Action Register (Empirically Falsified / Deliberate Designs)

These items surfaced during static sweeps and were falsified against live CPython
3.14 execution, source tracing, `docs/program-theory.md`, or archived git plans.
Reviewers and implementers must **not** "fix" them:

1. **Fork-inherited object retention lists** (`_INHERITED_PROCESS_SESSIONS_ON_FORK`
   in `simplebroker/_broker_session.py`, `_ABANDONED_FORK_CONNECTIONS` in PG/Redis,
   `_INHERITED_RESOLUTIONS`): Deliberate POSIX fork defense (`[REV-THEORY-005]`,
   `[SB-API-11]`) preventing child cyclic GC from running finalizers on parent
   file descriptors or locked mutexes.
2. **Cross-thread finalization poisoning without calling `rollback()`**
   (`simplebroker/db.py`, `docs/implementation/04-cross-thread-finalization-poisoning.md`,
   `[SB-DELIVERY-6]`): Deliberate C-driver/thread-ownership safety. Calling
   `rollback()` on a GC thread rolls back the wrong SQLite thread-local
   connection or violates `psycopg` thread ownership.
3. **2-statement pipelined PostgreSQL write (`LOCK_LAST_TS_ROW` +
   `WRITE_GENERATED_MESSAGE`)** (`extensions/simplebroker_pg/simplebroker_pg/plugin.py`):
   Deliberate lock-order (`meta` before `messages`) deadlock defense against
   concurrent `broadcast`. Do not collapse into a single SQL CTE.
4. **URI `mode=ro` SQLite header validation**
   (`simplebroker/_backends/sqlite/validation.py`): Deliberate POSIX `fcntl`
   advisory-lock preservation (`open(path, "rb").close()` strips process-wide
   inode locks).
5. **Subagent static claim of deterministic false `ResourceWarning`s and GC-thread
   `stop()` in `BaseWatcher.__del__`**: Empirically falsified on Python 3.14
   (internal `_strategy -> _notify_activity` cycle prevents refcount zero inside
   `Thread.run()`, and stack roots pin `self` while alive). Only the real
   unlocked `_thread` publication window and dead `__del__` residue (`10a`) are
   in scope.
6. **Lock-file `unlink()` in `examples/sqlite_connect.py` (`_release_lock`, line 865)**:
   Falsified during external plan review (`F4`): `lock_path.unlink()` already
   executes *only* when `not HAS_FCNTL and not HAS_MSVCRT` (the `O_CREAT | O_EXCL`
   existence-lock fallback at line 752); POSIX `fcntl` and Windows `msvcrt`
   already retain the lock inode, and removing the fallback `unlink()` would
   permanently wedge fallback locks.
7. **Coalescing `docs/lessons.md` compaction (61 entries vs 50 watermark)**:
   Explicitly `checked-deferred` in `docs/coalescing.md` and requires separate
   owner coalescing authorization per `AGENTS.md`. Do not compact `docs/lessons.md`
   in this remediation.

## Principle-Level Diagnosis

Clustering `Findings 1–10g` exposes four structural root causes that guide how
the fixes are designed (so we eliminate classes of bugs rather than patching
point symptoms):

1. **Multiplexed Resource State Without Scoped Restoration (`Findings 4, 10b, 10d`)**:
   Process-session core sharing (`[REV-THEORY-006]`) multiplexed multiple
   `Queue` handles onto one thread-local `BrokerCore`, while `RedisBrokerCore`
   can also be shared directly across threads, yet per-call mutable attributes
   (`_stop_event`, `_active_generator_batch` / `_active_generator_batch_owner`)
   remained flat overwrites instead of stack-restored or thread-keyed state.
2. **Post-Commit Side Effects & Split-Snapshot Metadata Reads (`Findings 3, 5, 10c`)**:
   Executing unretried network/SQL commands *after* a durable commit
   (`RedisBrokerCore._publish`, `refresh_last_timestamp()`) creates a false-failure
   boundary: the broker does *not* automatically replay committed rows on a
   post-commit failure (verified across `BrokerCore.insert_messages`,
   `Queue.insert_messages`, `load_lines`, and `RedisBrokerCore`), yet a transient
   lock or socket blip on that post-commit call surfaces an exception to the caller
   after rows are already committed (which can trigger caller-level duplicate sends
   on `write`/`broadcast`). Moving wakeup and `last_ts` observation *inside* the
   atomic unit (Lua script / SQL transaction), advancing `alias_version`
   monotonically (`+ 1` / `HINCRBY`), and reading SQL alias snapshots under a
   seqlock eliminates the split phase without weakening `TRANSPORT-AMBIGUOUS`
   mutation non-replay.
3. **Backend Extension Contract Asymmetry (`Findings 2, 3, 7, 10e`)**:
   SQLite and PostgreSQL enforced batch-looped vacuum, `OperationalError`
   translation, and strict option validation, while newer extension surfaces
   (`simplebroker_redis` non-write paths, `PostgresBackendPlugin` options)
   checked only happy-path keys/commands.
4. **Gate Scripts Validating Presence Rather Than Reachability/Totality (`Findings 1, 9`)**:
   Verification helpers (`coalesce-check`, `check-doc-paths`, `check-plan-context`,
   `check-dom15-fixtures`) exited `0` on loose git objects, shallow clones,
   dangling symlinks, first-match path resolution, or malformed CLI flags.

## Spec Baseline

Current canonical product-spec statements (`docs/specs/product-section-registry.md`
declares all touched families `canonical-spec`):

1. **`docs/specs/10-cli.md` `[SB-CLI-2]` (lines 91–95, 103–108, 140–149, 169–170)**:
   - Quiet mode suppresses owned human commentary on `stderr` without installing
     a blanket runtime-warning filter.
   - Ordinary plain-text errors use `simplebroker: error: <message>` derived
     from `PROG_NAME`.
   - Relative legacy-SQLite physical containment is currently required *"before
     backend command dispatch or a target-opening `--status`, `--vacuum`, or
     `--compact` action"*, while line 169 states *"`init` and `[SB-OPS-7]`
     cleanup retain their separately specified preparation and path behavior."*
2. **`docs/specs/10-cli.md` `[SB-CLI-4]` (lines 266–295)**:
   - Once argument parsing establishes JSON mode, every later ordinary
     validation, global-option, preparation, and dispatch error writes one
     structured JSON error object (`INVALID_ARGUMENT`, `INVALID_MESSAGE_ID`,
     `INVALID_TIMESTAMP`, or `ERROR`) and never falls back to bare `argparse`
     diagnostics.
   - `INVALID_ARGUMENT` covers parser-owned option/operand conflicts and rejected
     relative-path containment checks; `ERROR` covers failure to resolve a
     selected path or symlink.
3. **`docs/specs/17-ops.md` `[SB-OPS-7]` (lines 192–196)**:
   - *"For a SQLite filesystem target, one expanded, resolved path is frozen for
     validation and all owned-name derivation."* Currently silent on physical
     working-directory containment when `--cleanup` receives an ordinary
     relative legacy-SQLite `-f` target.
4. **`docs/specs/16-python-library-api.md` `[SB-API-2]` (lines 135–165)**:
   - Lines 135–146 deliberately scope `Queue(..., BrokerTarget(...))`
     construction-time option binding to Redis (leaving PostgreSQL and other
     plugin initializers lazy at `Queue` construction so core-only environments
     without `simplebroker-pg` installed and lazy plugin tests remain valid),
     while lines 156–165 assign `backend_options` validation and normalization
     ownership to each backend plugin without stating explicitly that PostgreSQL
     rejects unknown `backend_options` keys with `DatabaseError` across config
     initialization and runner/schema/waiter entry points before opening a
     connection.
   - Lines 148–150 specify that project-config loading emits a redacted advisory
     warning when the configured target string contains a recognized inline
     password (`password=` or URL userinfo password; `sslpassword=`, the exact
     RFC-3986 post-path query/fragment `@` overlap boundary, and CLI `--quiet`
     suppression of that stderr commentary are not yet stated).
5. **`docs/specs/16-python-library-api.md` `[SB-API-11]` (lines 1260–1287)**:
   - Line 1261 contains stale text *"First-party plugins declare v8 exactly"*,
     contradicted 24 lines below by line 1285 (*"First-party plugins declare v9
     exactly"*).

## Proposed Spec Delta

Under `docs/agent-context/runbooks/writing-plans.md` §4d **Strategy A**, the
**Spec-Promotion Slice** updates the canonical spec prose and `## Related Plans`
backlinks first (without forward-referencing not-yet-written test node IDs), and
the **Final Traceability Slice** adds `_Verification_` test node citations once
those tests exist:

1. **`docs/specs/10-cli.md` `[SB-CLI-2]`**:
   - Extend relative legacy-SQLite physical containment to `--cleanup`: for an
     ordinary relative legacy-SQLite target (neither explicit absolute `-f` nor
     project-scoped), `--cleanup` resolves symlinks on the target and working
     directory and enforces physical containment within the selected working
     directory before freezing the cleanup namespace, **without** requiring the
     working directory or target to exist (preserving `[SB-OPS-7]` missing-path
     `exit 0` no-ops and in-root symlink target cleanup while rejecting relative
     symlink escapes with `exit 1` / `INVALID_ARGUMENT`, and reporting symlink
     resolution loops as `ERROR` under `[SB-CLI-4]`). Update line 169 to state
     that `init` retains its current-directory initialization preparation while
     `[SB-OPS-7]` cleanup shares relative-target physical containment without
     requiring existing directories.
   - Clarify in `[SB-CLI-2]` that plain-text CLI `stderr` diagnostics and owned
     warnings escape control, escape, format, and lone-surrogate code points so
     hostile input cannot inject multi-line or terminal escape sequences, and
     that CLI `--quiet` also suppresses the owned `move` cap notice and CLI
     project-config inline-password commentary.
2. **`docs/specs/17-ops.md` `[SB-OPS-7]`**:
   - Align the SQLite filesystem path-freezing paragraph with `[SB-CLI-2]`:
     when invoked through CLI legacy relative-SQLite path mode, physical
     containment within the selected working directory is verified before any
     validation or owned-name derivation; an absent working directory or absent
     in-root target remains a `0`-exit no-op.
3. **`docs/specs/16-python-library-api.md` `[SB-API-2]` & `[SB-API-11]`**:
   - Clarify under `[SB-API-2]` plugin option ownership (lines 156–165, keeping
     lines 135–146 scoped to Redis `Queue` construction-time binding so
     non-Redis plugins remain lazy at `Queue` construction) that PostgreSQL
     rejects unknown `backend_options` keys with `DatabaseError` during
     `init_backend` / project-config loading and at runner, target-lifecycle,
     and activity-waiter entry points before opening any PostgreSQL connection.
   - State the inline-password detection and redaction contract in `[SB-API-2]`
     explicitly: recognized inline passwords include conninfo `password=` /
     `sslpassword=` parameters and URL userinfo passwords (continuing to fail
     closed by masking through the last `@` whenever `:` precedes `@` after
     `scheme://`). State that CLI `--quiet` suppresses the advisory stderr
     commentary during CLI invocations while direct Python
     `load_project_config()` calls continue to emit it.
   - Fix the stale sentence in `[SB-API-11]` (`16-python-library-api.md` line
     1261) from *"First-party plugins declare v8 exactly"* to historical tense
     (*"Backend API v8 added ...; see Backend API v9 below for the current
     first-party plugin version"*), eliminating the internal `v8` vs `v9`
     contradiction without bumping `BACKEND_API_VERSION = 9`.

## Invariants

1. **No Daemon / No Leases / Two-State Claim Model (`[THEORY-2]`, `[THEORY-6]`, `[ALT-THEORY-001]`)**:
   No background reaper threads, visibility timeouts, or third message states are
   introduced in any backend.
2. **Backend API Version Stability (`BACKEND_API_VERSION = 9`, `[SB-API-11]`)**:
   All fixes preserve the v9 `BrokerConnection` / `BackendPlugin` protocols
   (no new required protocol methods such as `get_stop_event`) and SQL schema v6
   layout (`[ALT-IMPL09-002]`) with zero schema migrations and zero breaking
   public API signature changes.
3. **Thread & Fork Ownership Invariants (`[REV-THEORY-005]`, `[REV-THEORY-006]`, `04-cross-thread-finalization-poisoning.md`, `06-process-session-core-ownership.md`)**:
   - Only the owning thread/PID touches live SQLite/psycopg/Redis connections.
   - Forked children never close or roll back parent handles and never inherit
     stale active generator batch guards or stale alias caches.
   - Cross-thread finalization of an unclosed `at_least_once` generator on SQL
     backends continues to poison `BrokerCore` rather than calling `rollback()`
     from the GC thread.
4. **Stack-Scoped Cooperative `_stop_event` Without Protocol Expansion (`Finding 4`)**:
   Every borrowed/leased connection scope (`_ProcessBrokerSession` thread-local
   operation stack and non-shared `DBConnection._operation_connection`) restores
   the previously active `_stop_event` on exit (`try ... finally`), even on
   exception or `GeneratorExit`, and reuses a per-instance default unset
   `threading.Event` when `stop_event is None` instead of allocating a fresh
   `Event` per call or adding methods to `BrokerConnection`.
5. **Single-Round-Trip Atomic Redis Mutation + Wakeup & Preserved `TRANSPORT-AMBIGUOUS` Boundary (`Finding 3`, Owner Q2)**:
   - Every mutating Redis Lua script (`WRITE_MESSAGE`, `INSERT_MESSAGES`,
     `BROADCAST_MESSAGE`, `MOVE_MESSAGES`, `COMMIT_MOVE_BATCH`, `RENAME_QUEUE`)
     performs queue `PUBLISH` inside the Lua script using `ARGV`-supplied channel
     names/prefixes (never `KEYS`, preserving Redis/Valkey Cluster hash-slot
     compatibility) and returns persisted `last_ts` where needed.
   - Exception translation on Redis (`_translate_redis_error(exc)`) converts
     `redis.RedisError` into `OperationalError` while setting `retryable = True`
     **only** on explicit server `BUSY ` script-busy responses — preserving
     `SM-REDIS-WRITE` `TRANSPORT-AMBIGUOUS` non-replay on uncertain transport
     errors in `Queue._execute_with_retry` while enabling `QueueWatcher`
     operational retries (`getattr(exc, "retryable", None) is not False`).
6. **Monotonic `alias_version` + Seqlock Snapshot Consistency (`Finding 5`, `F7`)**:
   `alias_version` strictly increases (`old_version + 1` in SQL; atomic Lua
   `HINCRBY meta alias_version 1` in Redis `ADD_ALIAS`, `RENAME_QUEUE` when
   aliases are retargeted, and `REMOVE_ALIAS`) on every committed alias mutation
   across SQLite, PostgreSQL, and Redis; SQL `_load_aliases_locked()` never tags
   an older `SELECT_ALIASES` snapshot with a newer `alias_version`; and
   rolled-back SQL alias transactions invalidate `_alias_cache_version = -1`.
7. **Fail-Closed Relative Containment Without Breaking Missing-Root Cleanup (`Finding 6`, Owner Q1)**:
   `broker -d /missing --cleanup` and `broker -f missing.db --cleanup` continue
   to exit `0` without creating directories; `alias.db -> actual.db` within the
   working root continues to clean `actual.db` and its sidecars while leaving
   `alias.db` intact; `link.db -> ../outside/victim.db` fails closed (`exit 1`,
   `INVALID_ARGUMENT`) with zero filesystem mutations; and `loop.db -> loop.db`
   fails closed (`exit 1`, `ERROR` per `[SB-CLI-4]`).
8. **Ruff Complexity & Approved Suppression Registry Governance (`[DOM-10.1]`, `F8`)**:
   `uv run --frozen ruff check .` and
   `uv run --frozen python bin/ruff_suppression_index.py --check` pass without
   adding unapproved `# noqa` directives, while preserving existing owner-approved
   complexity exceptions (`[RUFF-SUP-020]` on `RedisBrokerCore.find_message_ids` /
   `RedisBackendPlugin.cleanup_target` and `[RUFF-SUP-021]` on
   `RedisBrokerCore.broadcast`) and updating the generated line-location table
   via `bin/ruff_suppression_index.py --write` if line numbers shift inside
   already-registered files.

## Hidden Couplings & Observable-Difference Register

Every non-obvious coupling discovered during source verification and external
plan review is pinned here so slices do not trip hidden regressions:

| Surface | Hidden Coupling / Existing Test Expectation | Required Handling in Implementation |
| :--- | :--- | :--- |
| `simplebroker/watcher.py` (`run_in_thread`, `F1`) | `self._run_thread` initializes to `_WatcherLifecycleState.IDLE` (`watcher.py:411-413`), **not** `None`; `[SB-API-6]` (`16-python-library-api.md:822-825`) requires a `stop()` that wins before startup (`STOP_OWNED`) to prevent resource acquisition without raising `RuntimeError`. | Under `self._stop_lock`, read `state = self._lifecycle_state_locked()` and `prior = self._thread() if self._thread is not None else None`. Reject with `RuntimeError("Watcher is already running")` **only** when `state is _WatcherLifecycleState.RUN_OWNED` or `prior is not None and prior.is_alive()`. For `IDLE`, `RELEASED`, and `STOP_OWNED`, publish `self._thread = weakref.ref(thread)` (when `state is not STOP_OWNED` or `self._thread is None`) and call `thread.start()` under `_stop_lock`, resetting `self._thread` if `start()` raises. |
| `simplebroker/_targets.py` (`F2`, `HR-1`) | `tests/test_target_redaction.py` requires masking raw `/`, `?`, `#`, `@`, and spaces inside malformed passwords; `F2` proved URI exemptions create overlap classes (`scheme://u:5432/path?q@host`), and owner scoping review (`2026-11-10`) chose conservative fail-closed last-`@` masking. | Keep `_redact_raw_url_userinfo` unchanged (always fail-closed masking through the last `@`) and widen `_CONNINFO_PASSWORD_RE` to `r"(?i)(\b(?:ssl)?password\s*=\s*)..."`. |
| `simplebroker/_delivery.py` & generators (`F3`, `N1`) | `BrokerCore.{claim,peek,move}_generator` (`db.py:2818`) and `RedisBrokerCore.{claim,peek,move}_generator` accept `batch_size: int | None = None`; `[SB-API-4]` (`16-python-library-api.md:682-688`, `tests/test_connection_config.py:424`) requires `claim_generator` and `move_generator` to select `effective_config["GENERATOR_BATCH_SIZE"]` on first `at_least_once` iteration when `batch_size is None` (without resolving `config` in `exactly_once`), whereas `peek_generator` defaults `None` to `PEEK_BATCH_SIZE` (`1000`). | Keep `batch_size: int | None = None` on all signatures and make `default: int` a required keyword parameter on `resolve_generator_batch_size(batch_size: int | None, *, default: int) -> int`. In `claim_generator` and `move_generator` (SQL and Redis), validate explicit non-`None` `batch_size` upfront (`batch_size is not None` -> `resolve_generator_batch_size(batch_size, default=1)`) so `exactly_once` rejects invalid explicit values without resolving `config`, and resolve `effective_batch_size = resolve_generator_batch_size(batch_size, default=int(effective_config["GENERATOR_BATCH_SIZE"]))` in `at_least_once`; in `peek_generator` (SQL and Redis), resolve `resolve_generator_batch_size(batch_size, default=PEEK_BATCH_SIZE)`. |
| `simplebroker/_project_config.py` | `tests/test_project_config.py` (lines 123–157) captures `capsys.readouterr().err` on direct `load_project_config(config_path)` calls (`pytest` intercepts `warnings.warn`, so `print(..., file=sys.stderr)` is load-bearing per `[SB-CLI-2]` line 77). | Keep `print(..., file=sys.stderr)` in `_warn_for_inline_project_config_password`, sanitize `config_path`, and suppress only when SimpleBroker's explicit CLI `--quiet` `ContextVar` is active. |
| `simplebroker/_targets.py` (`BrokerTarget`) & PG option owners (`F6`, `N2`) | `BrokerTarget` is constructed with arbitrary dicts in `test_project_config.py:1304` and `test_target_redaction.py:113`; `tests/test_backend_plugin_resolution.py:209-228` asserts non-Redis plugin initializers stay lazy at `Queue(...)` construction (`"other plugin initializers must stay lazy"`), and `tests/test_queue_api_additions.py:249-256` constructs `Queue("tasks", db_path=BrokerTarget("postgres", ..., backend_options={"schema": "option-secret"}))` in core-only CI where `simplebroker-pg` is not installed. `PostgresBackendPlugin` has no `validate_backend_options` method; its live validation owners are `require_schema_name` (`validation.py:53`) and module-level `verify_env` (`plugin.py:275`). | Keep `BrokerTarget` a pure data carrier and leave `sbqueue._canonicalize_queue_target` lazy for `"postgres"` (do not touch `sbqueue.py` for Postgres). Reject unknown keys (`sorted(str(k) for k in opts if k != "schema")`) with `DatabaseError` inside `require_schema_name` (`extensions/simplebroker_pg/simplebroker_pg/validation.py:53`, covering `create_runner`, `initialize_target`, `validate_target`, `cleanup_target`, `create_activity_waiter`, and `create_activity_waiter_for_queues` before any DB connect) and inside module-level `verify_env(..., toml_options=...)` (`extensions/simplebroker_pg/simplebroker_pg/plugin.py:288`, covering `init_backend`, `.broker.toml` loading, and target resolution). |
| `simplebroker/cli.py` (`_run_cleanup`) | `[SB-CLI-4]` (`10-cli.md` lines 286, 292) assigns `INVALID_ARGUMENT` to rejected relative-path containment checks and `ERROR` to symlink resolution failures (`ELOOP`). `tests/test_cleanup.py` (lines 325–348) requires `alias.db -> actual.db` to clean `actual.db` + sidecars while keeping `alias.db` intact and status text naming `db_path`. | Route through `_resolve_legacy_sqlite_path` (`_ArgumentValidationError` -> `INVALID_ARGUMENT` on containment escape; `ValueError` -> `ERROR` on `ELOOP`), pass `str(resolved_db_path)` to `cleanup_target`, and keep `db_path` in `commands._status(...)`. |
| `simplebroker/db.py` & `simplebroker_redis/core.py` | `tests/test_generator_methods.py` (lines 336, 365) asserts `match="separate BrokerDB/Queue instance"` on nested generator `RuntimeError`. | Update `db.py`, `simplebroker_redis/core.py`, and `tests/test_generator_methods.py` together to accurately recommend `delivery_guarantee='exactly_once'`, an ephemeral `Queue(..., persistent=False)`, or an independent `open_broker()` handle. |
| `simplebroker_redis` Lua, `alias_version`, & state machines (`F6`, `F7`, `F8`) | `ADD_ALIAS` (`scripts.py:36`) and `RENAME_QUEUE` (`scripts.py:623`) currently `HSET` wall-clock timestamps; `SM-REDIS-WRITE` (`07-complexity-and-state-machine-map.md:429`), `README.md:76`, `test_redis_core_behaviors.py:163-186`, `test_redis_activity_waiter_lifecycle.py:65`, `test_redis_plugin_contract_edges.py:76`, and `test_redis_state_machine_transitions.py` reference `_publish`, `activity:all`, `_wildcard_version`, or Lua return shapes; `find_message_ids` and `broadcast` carry `[RUFF-SUP-020]` / `[RUFF-SUP-021]`. | Update `ADD_ALIAS`, `RENAME_QUEUE`, and new `REMOVE_ALIAS` to `HINCRBY meta alias_version 1` (removing timestamp `ARGV`s); pass Pub/Sub channels in `ARGV`; preserve `_translate_redis_error(exc)` `TRANSPORT-AMBIGUOUS` retryable semantics; update `SM-REDIS-WRITE` prose + all four Redis test modules; regenerate `10-ruff-suppression-registry.md` index locations if line numbers shift. |
| `docs/program-theory.md` & `bin/check-doc-paths` | `docs/program-theory.md` line 209 currently cites the retired `2026-07-29-complexity-and-state-machine-hardening-plan.md` with prose `at 2daa2fb48dd478fee5c01bec86add53793d55940` instead of `commit:path`; adding `docs/program-theory.md` to `bin/check-doc-paths` fails unless converted to canonical `commit:path` form. | Convert line 209 to `` `2daa2fb48dd478fee5c01bec86add53793d55940:docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md` `` (matching line 220; verified blob exists in git) in Slice 1. |
| `examples/dead_letter_queue.sh` (`F5`) | Lines 158–175 index `.retry_count` / `.message` and assign `.retry_count = ...`; `jq -e 'type'` admits JSON scalars (`false`, `null`, `0`, `"str"`, arrays) that then fail indexing with `jq` exit `5` or overwrite `null` into `{"retry_count": 1}`. | Require a JSON object (`printf '%s\n' "$raw_msg" | jq -e 'type == "object"' >/dev/null 2>&1`) at **both** line 158 and line 169 so non-object JSON scalars/arrays wrap into `{message: $msg, retry_count: 1}` cleanly. |

## Context and Required Reading

Before executing any slice, an implementing or reviewing agent must read the
files listed in `## Source Documents` for that slice and be able to answer these
comprehension checks from the source without guessing:

1. **Why must `Queue(..., persistent=True)` handles on the same thread share one
   `BrokerCore`, and how can nested `_stop_event` restoration work without
   adding `get_stop_event()` to `BrokerConnection` v9?**
   - *Expected answer*: Process-session core sharing (`06-process-session-core-ownership.md`,
     `[REV-THEORY-006]`) leases one `(pid, target, config)` thread-local
     `BrokerCore` across persistent `Queue` handles on a thread, tracked by
     `_ProcessBrokerSession._thread_local.operation_depth`. Pairing a
     `stop_event_stack` on `_ProcessBrokerSession._thread_local` (and restoring
     `self._stop_event` in `DBConnection._operation_connection` for non-shared
     managers, while keeping a per-instance `_default_stop_event` for `None`)
     restores the outer caller's `stop_event` using only existing
     `set_stop_event(...)`.
2. **Why does `_translate_redis_error(exc)` leave `OperationalError.retryable`
   unset (`None`) for `ConnectionError` / `TimeoutError` and set `retryable = True`
   only for `BUSY `, and why does embedding `PUBLISH` + `persisted_last_ts` in
   Lua still eliminate a real bug?**
   - *Expected answer*: `Queue._execute_with_retry` (`_retry_policy.py:188`)
     retries only when `retryable is True` (or SQLite lock text), preserving
     `SM-REDIS-WRITE` `TRANSPORT-AMBIGUOUS` non-replay on uncertain socket drops,
     whereas `QueueWatcher` (`_retry_policy.py:198`) retries whenever
     `isinstance(exc, OperationalError)` and `retryable is not False` (which raw
     `redis.RedisError` bypassed!). Meanwhile, post-commit `_publish` /
     `refresh_last_timestamp` could fail *after* Lua committed (surfacing a
     false-negative failure on an already-durable write; probe confirmed
     `refresh_last_timestamp` is outside broker retry loops rather than a
     duplicate-insert retry path). Moving `PUBLISH` (via `ARGV` for Cluster slot
     safety) and `persisted_last_ts` into Lua makes commit + wakeup + floor
     observation one atomic script call.
3. **Why did checking `self._run_thread is not None` fail on fresh watchers, and
   why must `STOP_OWNED` not raise `RuntimeError` in `run_in_thread()`?**
   - *Expected answer*: `BaseWatcher.__init__` sets
     `self._run_thread = _WatcherLifecycleState.IDLE` (not `None`), and
     `[SB-API-6]` (`16-python-library-api.md:822-825`) specifies that if
     `stop()` wins before startup (`STOP_OWNED`), the later startup must return
     without acquiring runtime resources rather than raising an unhandled error.
     Only `RUN_OWNED` (or an already-live `_thread`) is a duplicate active run.
4. **Why does `[SB-CLI-4]` map a relative symlink escape in `--cleanup` to
   `INVALID_ARGUMENT` (`exit 1`) but a symlink loop (`ELOOP`) to `ERROR` (`exit 1`)?**
   - *Expected answer*: `[SB-CLI-4]` (`docs/specs/10-cli.md` lines 286 and 292)
     explicitly classifies *"a rejected relative-path containment check"* as
     `INVALID_ARGUMENT` and *"failure to resolve a selected path or symlink"* as
     `ERROR`. Routing `_run_cleanup` through `_resolve_legacy_sqlite_path`
     preserves both classifications identically to `_validate_legacy_sqlite_target`.

## Rollout, Compatibility, One-Way Doors, and Rollback

- **One-way doors**: None. SQL storage schema remains v6; Redis keyspace layout
  is unchanged (`activity:all` had zero active subscribers in
  `_SharedRedisActivityListener`, which already subscribed exclusively to
  `activity:q:<queue>` channels); `BACKEND_API_VERSION` remains `9`.
- **Behavioral tightening (`[SB-API-2]`, `[SB-CLI-2]`, `[SB-CLI-4]`)**:
  - Postgres configurations passing unknown keys in `backend_options` now raise
    `DatabaseError` immediately instead of silently ignoring them (fixing a
    tenant-isolation hazard on typoed `schema`).
  - Relative CLI `--cleanup` targets that resolve via symlinks outside the
    selected working directory now fail closed (`exit 1`, `INVALID_ARGUMENT`)
    instead of deleting external databases; intentional external cleanup still
    works via explicit absolute `-f /abs/path/to/broker.db --cleanup` or
    `.broker.toml` project targets.
  - `broker move <src> <dst> -m <id> --all --json` now exits `1` with JSON
    `INVALID_ARGUMENT` instead of `argparse` exit `2` plain text (matching `read`
    and `peek`).
- **Rollback**: Each slice is self-contained and revertable cleanly via `git revert`
  without data migration or manual state repair.
- **Post-merge / CI success signals**:
  - GitHub Actions `lint` job runs `bin/coalesce-check`, `bin/check-doc-paths`,
    `bin/check-plan-context`, and `python bin/check-dom15-fixtures` on a
    full-history checkout (`fetch-depth: 0`) and exits `0`.
  - Full core, PostgreSQL, Valkey/Redis, state-machine manifest, Ruff suppression,
    and mypy cohorts pass green.

## Tasks

### Spec-Promotion Slice — Canonical `[SB-*]` Clarifications & Backlinks (Strategy A)

- **Objective**: Land the normative spec updates in `## Proposed Spec Delta` and
  add `## Related Plans` backlinks before code changes, without adding
  not-yet-existing test node citations so `tests/test_doc_gates.py` stays green.
- **Files**:
  - `docs/specs/10-cli.md` (`[SB-CLI-2]`, `## Related Plans`)
  - `docs/specs/16-python-library-api.md` (`[SB-API-2]`, `[SB-API-11]`, `## Related Plans`)
  - `docs/specs/17-ops.md` (`[SB-OPS-7]`, `## Related Plans`)
- **Steps**:
  1. Edit `docs/specs/10-cli.md` `[SB-CLI-2]` to:
     - include `--cleanup` on ordinary relative legacy-SQLite targets in physical
       working-directory containment (without requiring the working directory or
       target to exist, preserving `[SB-OPS-7]` absent-namespace `exit 0` no-ops);
     - state that plain-text CLI `stderr` diagnostics and owned warnings escape
       terminal control/escape/format/surrogate characters into single-line safe
       output;
     - list the `move` cap notice and CLI project-config inline-password
       commentary under owned `stderr` commentary suppressed by `--quiet`.
  2. Edit `docs/specs/17-ops.md` `[SB-OPS-7]` to align legacy relative-SQLite
     `--cleanup` path freezing with `[SB-CLI-2]` containment.
  3. Edit `docs/specs/16-python-library-api.md` `[SB-API-2]` to clarify under
     plugin option ownership (lines 156–165, keeping lines 135–146 scoped to
     Redis `Queue` construction-time binding) that PostgreSQL rejects unknown
     `backend_options` keys with `DatabaseError` across config initialization
     and runner/target/waiter entry points before opening a connection, state
     the `sslpassword=` and conservative RFC-3986 post-path query/fragment
     inline-password boundary (`HR-1`) + CLI `--quiet` behavior, and fix the
     stale `v8` sentence at line 1261 in `[SB-API-11]`.
  4. Add `- docs/plans/2026-11-10-deep-dive-review-remediation-plan.md` under
     `## Related Plans` in `10-cli.md`, `16-python-library-api.md`, and
     `17-ops.md`.
- **Verification**:
  - `./bin/check-doc-paths`
  - `./bin/check-plan-context`
  - `python3 ./bin/check-dom15-fixtures`
  - `uv run --frozen pytest tests/test_doc_gates.py`

### Slice 1 — Governance, Coalescing Ledger & CI Gate Hardening (Findings 1, 9)

- **Objective**: Repair the live clean-clone failure on `main` and close every
  verified false-pass defect in the repository's governance and release scripts.
- **Files**:
  - `docs/coalescing.md`
  - `docs/program-theory.md`
  - `bin/coalesce-check`
  - `bin/check-doc-paths`
  - `bin/check-plan-context`
  - `bin/check-dom15-fixtures`
  - `.github/workflows/test.yml`
  - `.github/scripts/require_green_workflows.py`
  - `tests/test_doc_gates.py`
  - `tests/test_release_script.py`
- **TDD Steps (RED -> GREEN)**:
  1. **RED tests first** in `tests/test_doc_gates.py` and
     `tests/test_release_script.py` (plus `--self-test` cases inside the `bin/*`
     checkers):
     - `coalesce-check`: synthetic git repo where a cited SHA exists only as an
       unreachable loose object (after branch reset/rebase) fails with exit `1`
       by default (`unreachable` / `local-only` rejected unless
       `--allow-local-only` is explicitly passed); a broken SHA within 80 chars
       of the word `"simplebroker"` fails with exit `1` instead of being skipped
       as a foreign marker.
     - `check-doc-paths`: dangling symlink target fails with exit `1`; broken
       path inside `docs/program-theory.md` or `docs/implementation/*.md` fails
       with exit `1`; backticked path with `#anchor`, `:line`, or `::symbol`
       suffix (`foo.py:12`, `README.md#section`) strips the suffix and validates
       the underlying repo path (failing if the file is missing).
     - `check-plan-context`: in-flight Class 3/4/5 plan missing
       `## Independent Review Loop` fails when primary status is `active` or
       `status-review` (while `draft` plans remain valid during pre-review
       authoring and have `## Independent Review Loop` validated whenever the
       section is present), AND `## Source Documents` with one valid path plus
       one broken concrete repo path fails with exit `1` instead of
       short-circuiting on the first valid path.
     - `check-dom15-fixtures`: `--spec=/nonexistent.md`, `--spec` with missing
       value, and `--unknown-flag` exit `2` instead of silently checking
       `DEFAULT_SPEC`.
     - `require_green_workflows.py` (`github_api_get`): transient `HTTPError`
       (`429`, `502`, `503`, `504`), `URLError`, and `TimeoutError` during
       `urlopen` / `json.load` retry with bounded backoff (`Retry-After` honored
       when numeric) before raising `RuntimeError` (preserving the exception
       type caught by `main()`), while `401`/`403`/`404` fail fast with
       `RuntimeError`.
  2. **GREEN implementation**:
     - Update `docs/coalescing.md` lines 154–155 (`e04c27a` -> `ee0d96c`,
       `f78612e` -> `2b9f7dd`) and convert `docs/program-theory.md` line 209 to
       `2daa2fb48dd478fee5c01bec86add53793d55940:docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`.
     - Update `bin/coalesce-check`: remove `"simplebroker"` from
       `FOREIGN_MARKERS`, require reachable ancestry from `HEAD` or the default
       published `remote_ref` (`origin/main`) — never arbitrary local branches
       or tags — unless `--allow-local-only` is passed, and tighten
       foreign-marker matching to explicit foreign repo prefixes.
     - Update `bin/check-doc-paths`: require `target.exists()` (rejecting
       dangling symlinks), add `docs/program-theory.md` to `SCAN_FILES` and
       `docs/implementation` to `SCAN_DIRS`, and strip `#fragment`, `:line(-end)`,
       and `::symbol` suffixes before resolving concrete repo file claims.
     - Update `bin/check-plan-context`: validate *every* concrete backticked
       repo path claim in `## Source Documents` (while still requiring at least
       one resolving path or explicit `Source spec: None — ...` waiver), and
       require `## Independent Review Loop` on `active` and `status-review`
       plans of effective class ≥ 3.
     - Update `bin/check-dom15-fixtures` to parse CLI arguments with `argparse`
       and exit `2` on unknown/missing/unreadable `--spec` arguments.
     - Update `.github/workflows/test.yml` (`lint` job) to checkout with
       `fetch-depth: 0` and run `bin/coalesce-check`, `bin/check-doc-paths`,
       `bin/check-plan-context`, and `python bin/check-dom15-fixtures`.
     - Add bounded transient retry in `.github/scripts/require_green_workflows.py`
       `github_api_get` (raising `RuntimeError` on terminal failure).
- **Verification**:
  - `./bin/coalesce-check && ./bin/coalesce-check --self-test`
  - `./bin/check-doc-paths`
  - `./bin/check-plan-context && ./bin/check-plan-context --self-test`
  - `python3 ./bin/check-dom15-fixtures`
  - Clean clone proof in temp dir: `git clone --no-local . "$TMP_CLONE"` and run
    `uv run --frozen pytest tests/test_doc_gates.py tests/test_release_script.py`.

### Slice 2 — Core Lifecycle, SQL Alias Seqlock, Containment, Redaction & CLI Polish (Findings 4, 5, 6, 8, 10a, 10c, 10d, 10e-core, 10f)

- **Objective**: Fix same-thread `_stop_event` clobbering, SQL alias cache
  staleness, post-commit SQL `refresh_last_timestamp()`, unlocked `status()`
  size query, `BaseWatcher.run_in_thread` synchronization, generator
  `batch_size` validation, `--cleanup` relative containment, `_targets.py`
  redaction, and CLI `--json` / `stderr` / `--quiet` ergonomics.
- **Files**:
  - `simplebroker/sbqueue.py`
  - `simplebroker/_broker_session.py`
  - `simplebroker/db.py`
  - `simplebroker/_timestamp.py`
  - `simplebroker/watcher.py`
  - `simplebroker/_delivery.py`
  - `simplebroker/_targets.py`
  - `simplebroker/_project_config.py`
  - `simplebroker/cli.py`
  - `simplebroker/commands.py`
  - `tests/test_broker_session.py`
  - `tests/test_watcher_cleanup.py`
  - `tests/test_aliases_db.py`
  - `tests/test_generator_methods.py`
  - `tests/test_cleanup.py`
  - `tests/test_target_redaction.py`
  - `tests/test_project_config.py`
  - `tests/test_cli_contract_sb_cli.py`
- **TDD Steps (RED -> GREEN, with `--cleanup` containment FIRST)**:
  1. **Step 2a — `--cleanup` containment FIRST (`Finding 6`)**:
     - **RED (`tests/test_cleanup.py`)**: relative `link.db -> ../outside/victim.db`
       with `broker -f link.db --cleanup` (both plain and `--json`) exits `1`
       with `INVALID_ARGUMENT` and leaves `../outside/victim.db` intact;
       relative `loop.db -> loop.db` with `broker -f loop.db --cleanup --json`
       exits `1` with `ERROR`; `broker -d /missing --cleanup` and in-root
       `alias.db -> actual.db` continue to exit `0`.
     - **GREEN (`simplebroker/cli.py`)**: Update `_run_cleanup` to compute
       `containment_required = not (Path(args.file).is_absolute() or resolved_target.used_project_scope)`,
       validate `args.file` and `db_path` (wrapping `ValueError` in
       `_ArgumentValidationError`), resolve via `_resolve_legacy_sqlite_path`,
       validate `resolved_db_path`, and pass `str(resolved_db_path)` to
       `cleanup_target` while keeping `db_path` in `commands._status(...)`.
  2. **Step 2b — Remaining Slice 2 RED tests**:
     - `tests/test_broker_session.py`: persistent `inbox` with `stop_event` on
       the same thread as persistent `outbox` (`stop_event=None`) and injected-runner
       `Queue` equivalents; after `outbox.write(...)` runs inside
       `inbox.read_generator()`, external SQLite `BEGIN IMMEDIATE` lock +
       `stop_event.set()` at `50 ms` raises `StopException` in `< 1.0 s`
       (proving `outbox` did not clobber `inbox`'s `stop_event`), and nested
       `stop_event` scopes restore the outer event on both normal exit and
       exception.
     - `tests/test_watcher_cleanup.py`: fresh watcher `run_in_thread()` /
       `start()` / `with watcher:` starts normally from `IDLE`; concurrent
       `run_in_thread()` vs `stop(join=True)` joins the started worker thread
       deterministically; `stop()` winning before `run_in_thread()` (`STOP_OWNED`)
       returns a thread that exits without acquiring runtime resources or
       raising `RuntimeError` (`[SB-API-6]`); calling `run_in_thread()` a second
       time while `RUN_OWNED` raises `RuntimeError` without overwriting
       `_thread`; `"__del__" not in BaseWatcher.__dict__` is `True`.
     - `tests/test_aliases_db.py`: deterministic fault/interleave probe between
       `SELECT_ALIASES` and `read_alias_version` (seqlock retries and never
       caches stale aliases under the new version); frozen `time.time_ns()`
       across `add_alias` + `remove_alias` still increments `alias_version`
       monotonically and invalidates peer `BrokerDB` caches; simulated `commit()`
       failure in `add_alias` / `remove_alias` / `rename_queue` invalidates
       `_alias_cache_version` so uncommitted aliases are never served; transient
       `OperationalError(..., retryable=True)` on `add_alias` / `remove_alias`
       retries via `_run_with_retry`.
     - `tests/test_generator_methods.py`: default `batch_size=None` calls on
       `Queue.read_generator()` and `BrokerCore.{claim,peek,move}_generator`
       continue to preserve operation-specific defaults
       (`effective_config["GENERATOR_BATCH_SIZE"]` for `at_least_once`
       `claim_generator`/`move_generator` per `[SB-API-4]` and
       `tests/test_connection_config.py`, and `PEEK_BATCH_SIZE` for
       `peek_generator`), while explicit `batch_size=0`, `batch_size=-1`,
       `batch_size=True`, and `batch_size="10"` raise `ValueError` in both
       `exactly_once` and `at_least_once` modes; nested re-entrancy
       `RuntimeError` names `delivery_guarantee='exactly_once'`,
       `Queue(..., persistent=False)`, and `open_broker()`.
     - `tests/test_target_redaction.py` & `tests/test_project_config.py`:
       `redact_backend_target` redacts `sslpassword=` in keyword/value conninfo
       strings (`host=db user=u sslpassword=mysecret`, quoted and escaped values)
       while `_backend_target_has_password` returns `True` (`load_project_config`
       emits the inline-password advisory), and URL-form targets keep existing
       fail-closed last-`@` masking across `postgresql://user:secret@localhost:5432/mydb?app=worker@prod`,
       `postgresql://user:1234/secret@host/db`, and `postgresql://audit:FAKE_PASSWORD extra@[bad/db`.
     - `tests/test_cli_contract_sb_cli.py`: `broker move q dst -m <19d> --all --json`
       exits `1` with valid JSON `{"error": "INVALID_ARGUMENT", ...}` on
       `stderr` and empty `stdout`; `broker -f $'bad\x1b[31m\nname.db' list`
       escapes `\x1b` and `\n` into one single-line `simplebroker: error: ...`
       diagnostic; `--quiet` suppresses the `cmd_move` cap commentary and CLI
       `.broker.toml` inline-password commentary while keeping direct
       `load_project_config()` stderr output intact.
  3. **Step 2c — Remaining Slice 2 GREEN implementation**:
     - **Stack-scoped `_stop_event` without protocol expansion (`Finding 4`, `F6`)**:
       - Give `DBConnection`, `BrokerCore`, and `RedisBrokerCore` a per-instance
         `self._default_stop_event = threading.Event()` so `set_stop_event(None)`
         restores `self._default_stop_event` without allocating a new `Event` on
         every operation.
       - In `_ProcessBrokerSession` (`simplebroker/_broker_session.py`), pair
         `self._thread_local.stop_event_stack: list[threading.Event | None]` with
         `operation_depth`: push `stop_event` and call `core.set_stop_event(stop_event)`
         in `get_connection(..., lease_operation=True)`, and pop/re-apply the
         outer `stop_event_stack[-1]` onto the thread's cached `core` inside
         `_end_operation` (`try ... finally`).
       - For non-shared `DBConnection` (`_share_in_process=False`), pass the
         operation's `stop_event` into `_operation_connection(stop_event=...)`
         (from `Queue.get_connection`) so `DBConnection` saves and restores its
         prior `_stop_event` in `finally` using existing `set_stop_event(...)`.
     - **SQL alias seqlock, monotonic version & retry (`Finding 5`)**:
       In `BrokerCore._load_aliases_locked()`, read `v1 = read_alias_version` ->
       `rows = SELECT_ALIASES` -> `v2 = read_alias_version` (up to 3 bounded
       passes if `v1 != v2`; if all 3 contend, store `rows` with
       `_alias_cache_version = -1` so the next call re-verifies rather than
       caching a torn version). Change `_increment_alias_version_locked` to
       `current = self._backend_plugin.read_alias_version(self._runner); new_version = current + 1`.
       Wrap `add_alias` and `remove_alias` transactions in `self._run_with_retry`,
       and set `self._alias_cache_version = -1` in the `except BaseException:`
       rollback handlers of `add_alias`, `remove_alias`, and `rename_queue`.
     - **Watcher thread publication & `__del__` removal (`Finding 10a`, `F1`)**:
       In `BaseWatcher.run_in_thread`, acquire `self._stop_lock`, inspect
       `state = self._lifecycle_state_locked()` and `prior = self._thread() if self._thread is not None else None`,
       raise `RuntimeError("Watcher is already running")` if
       `state is _WatcherLifecycleState.RUN_OWNED` or `prior is not None and prior.is_alive()`,
       create `thread`, publish `self._thread = weakref.ref(thread)` when
       `state is not _WatcherLifecycleState.STOP_OWNED` (or `self._thread is None`),
       and call `thread.start()` inside `_stop_lock` (resetting `self._thread` if
       `thread.start()` raises). Delete `BaseWatcher.__del__`.
     - **Generator `batch_size` validator & re-entrancy message (`Findings 10d, 10e`, `F3`, `N1`)**:
       Add `resolve_generator_batch_size(batch_size: int | None, *, default: int) -> int`
       in `simplebroker/_delivery.py` (with `default` required; returning
       `default` when `batch_size is None`, and rejecting `bool`, non-`int`, or
       `< 1` with `ValueError("batch_size must be a positive integer")`). In
       `BrokerCore.claim_generator` and `BrokerCore.move_generator`, validate
       explicit `batch_size` upfront (`if batch_size is not None: resolve_generator_batch_size(batch_size, default=1)`)
       so `exactly_once` rejects invalid explicit values without resolving
       `config`, and resolve
       `effective_batch_size = resolve_generator_batch_size(batch_size, default=int(effective_config["GENERATOR_BATCH_SIZE"]))`
       inside the `at_least_once` branch (`[SB-API-4]`). In
       `BrokerCore.peek_generator`, resolve
       `effective_batch_size = resolve_generator_batch_size(batch_size, default=PEEK_BATCH_SIZE)`.
       Update the re-entrancy `RuntimeError` text in `simplebroker/db.py` and
       `tests/test_generator_methods.py`.
     - **Target redaction (`Finding 8`, `HR-1`)**: Widen `_CONNINFO_PASSWORD_RE`
       in `simplebroker/_targets.py` to `r"(?i)(\b(?:ssl)?password\s*=\s*)(?:...)"`
       while leaving `_redact_raw_url_userinfo` fail-closed through the last `@`.
     - **CLI polish (`Finding 10f`)**:
       - Remove `mutually_exclusive_group()` on `move` (`--all` vs `-m`) in
         `simplebroker/cli.py` and enforce the conflict in
         `_validate_selection_filters_before_target` so `broker move ... -m <id> --all --json`
         emits structured `INVALID_ARGUMENT` JSON (matching `read` and `peek`).
       - Add `_sanitize_terminal_text(text: str) -> str` escaping C0, DEL, C1
         (`0x80..0x9f`), Unicode line/paragraph separators and format controls,
         and lone surrogates (`0xd800..0xdfff`), applied to plain-text `stderr`
         errors and warnings in `commands.py`, `cli.py`, and `_project_config.py`.
       - Honor `_MESSAGE_NEWLINE_WARNING_SUPPRESSED` (widened around `_main`
         target resolution + command dispatch when `args.quiet` is true) in
         `commands.cmd_move` (also replacing hardcoded `"broker"` with
         `PROG_NAME`) and `_project_config._warn_for_inline_project_config_password`.
     - **In-transaction `last_ts` floor & locked `status()` (`Finding 10c`, at end of Slice 2)**:
       Add `TimestampGenerator._publish_committed_floor(persisted_last_ts: int)`
       (advancing `self._last_ts = max(self._last_ts, persisted_last_ts)` and
       `self._initialized = True` under `self._lock`). In
       `BrokerCore.insert_messages`, read `persisted_last_ts` inside
       `_do_insert_messages_transaction` before `commit()` and publish that
       committed floor after `_run_with_retry` returns, deleting the post-commit
       `refresh_last_timestamp()` query. In `BrokerCore.status()`, move
       `database_size_bytes(self._runner)` inside `self._lock` and
       `self._run_with_retry`.
- **Verification**:
  - `uv run --frozen pytest tests/test_broker_session.py tests/test_watcher_cleanup.py tests/test_watcher_stop_contract.py tests/test_watcher_transition_tables.py tests/test_aliases_db.py tests/test_generator_methods.py tests/test_connection_config.py tests/test_cleanup.py tests/test_target_redaction.py tests/test_project_config.py tests/test_cli_contract_sb_cli.py`
  - `uv run --frozen ruff check simplebroker tests`
  - `uv run --frozen python bin/ruff_suppression_index.py --check`

### Slice 3 — PostgreSQL & Valkey/Redis Backend Extensions (Findings 2, 3, 5-redis, 7, 10b, 10c-redis, 10d-redis, 10e)

- **Objective**: Bring `simplebroker_pg` and `simplebroker_redis` to full
  contract parity on option validation, multi-batch vacuum, exception
  translation (preserving `TRANSPORT-AMBIGUOUS`), atomic Lua `PUBLISH` +
  `persisted_last_ts` return, monotonic `HINCRBY` `alias_version` across all
  alias scripts, thread/fork generator batch state, and Redis target/client
  initialization.
- **Files**:
  - `extensions/simplebroker_pg/simplebroker_pg/validation.py`
  - `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py`
  - `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py`
  - `extensions/simplebroker_redis/simplebroker_redis/keys.py`
  - `extensions/simplebroker_redis/simplebroker_redis/scripts.py`
  - `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
  - `extensions/simplebroker_redis/simplebroker_redis/validation.py`
  - `extensions/simplebroker_redis/README.md`
  - `docs/implementation/07-complexity-and-state-machine-map.md` (`SM-REDIS-WRITE`)
  - `docs/implementation/10-ruff-suppression-registry.md` (if line numbers shift)
  - `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`
  - `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py`
  - `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py`
  - `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`
  - `extensions/simplebroker_redis/tests/test_redis_atomicity.py`
- **TDD Steps (RED -> GREEN)**:
  1. **RED tests first**:
     - `test_pg_plugin_contract_edges.py` & `test_pg_schema_validation_paths.py`:
       `require_schema_name({"schema": "tenant_a", "schmea": "typo"})`,
       `require_schema_name({"schmea": "tenant_a"})`, module-level
       `verify_env(config, toml_target=dsn, toml_options={"schmea": "tenant_a"})`,
       `plugin.init_backend(config, toml_target=dsn, toml_options={"schmea": "tenant_a"})`,
       `plugin.create_runner(dsn, backend_options={"schema": "tenant_a", "extra": 1})`,
       `plugin.initialize_target(dsn, backend_options={"extra": 1})`,
       `plugin.validate_target(dsn, backend_options={"extra": 1})`,
       `plugin.cleanup_target(dsn, backend_options={"extra": 1})`, and
       `plugin.create_activity_waiter(target=dsn, backend_options={"extra": 1}, queue_name="jobs", stop_event=threading.Event())`
       all raise `DatabaseError` naming `"Unknown Postgres backend option(s): ..."`
       before opening any PostgreSQL connection, while
       `Queue("tasks", db_path=BrokerTarget("postgres", dsn, backend_options={"schema": "tenant_a"}))`
       keeps `Queue` construction lazy (preserving
       `tests/test_backend_plugin_resolution.py` and
       `tests/test_queue_api_additions.py`) and rejects unknown options on first
       runner/waiter creation.
     - `test_redis_core_behaviors.py` & `test_redis_atomicity.py`:
       - Seeding `2,500` claimed messages in one queue and calling
         `core.vacuum()` drains all `2,500` messages across 3 Lua batches,
         deletes all payload hashes, removes the drained queue from `queues`,
         and aborts cleanly between batches with `StopException` if
         `stop_event.is_set()`.
       - Injecting `redis.ConnectionError` / `redis.BusyLoadingError` /
         `redis.TimeoutError` / `redis.ResponseError("BUSY ...")` across
         `RedisBrokerCore` methods (`peek_one`, `peek_many`, `claim_one` during
         `_maybe_recover_stale_batches`, `list_queues`, `get_queue_stats`,
         `get_overall_stats`, `count_claimed_messages`, `find_message_ids`,
         `has_pending_messages`, `latest_pending_timestamp`, `recover_stale_batches`,
         `get_alias_version`, `resolve_alias`, `has_alias`, `list_aliases`,
         `remove_alias`, `get_meta`) raises SimpleBroker `OperationalError` via
         `_translate_redis_error(exc)` (`retryable is True` only for `BUSY `,
         `None` otherwise so `QueueWatcher` retries while `Queue` preserves
         `TRANSPORT-AMBIGUOUS` non-replay) — never raw `redis.RedisError`.
       - Real Pub/Sub subscriber / activity waiter receives queue wakeup on
         `write`, `insert_messages`, `broadcast`, `move_one`, `move_generator`
         commit, and `rename_queue`, stubbing `core._client.publish` to raise
         proves no standalone post-commit client `PUBLISH` can fail a committed
         write (testing behavior rather than pinning `INFO commandstats`
         `EVAL` counts), and zero `activity:all` publications are emitted;
         `insert_messages` and `broadcast` advance `_timestamp_gen` floor even
         when post-commit `hget` is stubbed to raise.
       - Concurrent `at_least_once` generators on two threads sharing one direct
         `RedisBrokerCore` isolate their active batch guards per thread (same
         thread still raises the updated re-entrancy `RuntimeError`; sibling
         thread succeeds cleanly), `_check_fork_safety()` clears inherited
         active batch guards in a child process, default `batch_size=None`
         generators preserve operation-specific defaults
         (`effective_config["GENERATOR_BATCH_SIZE"]` for `claim_generator` /
         `move_generator` per `[SB-API-4]` and `PEEK_BATCH_SIZE` for
         `peek_generator`), and explicit `batch_size=0`, `-1`, `True`, or
         `"10"` raises `ValueError` in both `exactly_once` and `at_least_once`
         modes.
       - Under a frozen/backward `time.time_ns()`, `add_alias`, `rename_queue`
         (when retargeting aliases), and `remove_alias` all atomically increment
         `meta.alias_version` by `+1` via Lua `HINCRBY`.
     - `test_redis_plugin_contract_edges.py`:
       - `init_backend` formats IPv6 `BACKEND_HOST="::1"` as
         `redis://[::1]:6379/0`, honors `BACKEND_USER` + `BACKEND_PASSWORD`,
         supplements `BACKEND_PASSWORD` onto passwordless explicit `.broker.toml`
         / `BACKEND_TARGET` Redis URLs, and redacts credentials if URL parsing
         raises `DatabaseError`.
       - `validation.connect()` closes the constructed `Redis` client if
         `client.ping()` raises `RedisError`.
  2. **GREEN implementation**:
     - **Postgres `backend_options` validation (`Finding 7`, `F6`, `N2`)**:
       Update the two existing PostgreSQL option validation owners without
       expanding the `BackendPlugin` v9 protocol or touching
       `simplebroker/sbqueue.py` (`Queue` construction stays lazy for `"postgres"`):
       - In `extensions/simplebroker_pg/simplebroker_pg/validation.py`
         (`require_schema_name`, line 53), check
         `unknown = sorted(str(k) for k in backend_options if k != "schema")`
         first and raise
         `DatabaseError(f"Unknown Postgres backend option(s): {', '.join(unknown)}")`
         before validating `"schema"` (hardening `create_runner`,
         `inspect_schema` / `initialize_target` / `validate_target` /
         `cleanup_target`, `create_activity_waiter`, and
         `create_activity_waiter_for_queues` before any database connection is
         opened).
       - In `extensions/simplebroker_pg/simplebroker_pg/plugin.py` (module-level
         `verify_env`, line 288, which backs `PostgresBackendPlugin.init_backend`
         at line 495), check
         `unknown = sorted(str(k) for k in toml_opts if k != "schema")` and raise
         the same `DatabaseError` before resolving `schema_source` (preventing
         `.broker.toml` or `init_backend` from silently ignoring typos and
         falling back to `"simplebroker_pg_v1"` or `config["BACKEND_SCHEMA"]`).
     - **Redis multi-batch `vacuum()` (`Finding 2`)**: Loop `VACUUM_CLAIMED` per
       queue while `int(deleted) == batch_size`, checking `self._stop_event`
       before each iteration (raising `StopException` when set), translating
       `redis.RedisError` via `_translate_redis_error(exc)`.
     - **Redis 3-step wakeup/floor decomposition & monotonic `alias_version` (`Findings 3, 5, 10c`, `F7`, Owner Q2 & Scoping Review)**:
       1. Drop the dead `activity:all` wildcard publish first (`RedisKeys.activity_all`,
          `_SharedRedisActivityListener._wildcard_version`,
          `_QueueWaiterRegistration.wildcard_version`, and the wildcard line in
          `RedisBrokerCore._publish`).
       2. Move per-queue `redis.call('PUBLISH', activity_channel, queue)` (using
          `ARGV` channel/prefix arguments, never `KEYS`, for Cluster slot safety)
          into `WRITE_MESSAGE`, `INSERT_MESSAGES`, `BROADCAST_MESSAGE`,
          `MOVE_MESSAGES`, `COMMIT_MOVE_BATCH`, and `RENAME_QUEUE` in `scripts.py`,
          and delete `RedisBrokerCore._publish`.
       3. As a separate small step, have `INSERT_MESSAGES` return
          `{1, persisted_last_ts}` and `BROADCAST_MESSAGE` return
          `{1, persisted_last_ts, #queue_names}`, calling
          `self._timestamp_gen._publish_committed_floor(int(result[1]))` in
          `RedisBrokerCore` and deleting post-commit `refresh_last_timestamp()`
          calls.
       4. Change `ADD_ALIAS` (`scripts.py:36`) and `RENAME_QUEUE`
          (`scripts.py:623`, when `aliases_retargeted > 0`) from wall-clock `HSET`
          to `redis.call('HINCRBY', meta, 'alias_version', 1)` (removing their
          obsolete timestamp `ARGV` arguments), add `REMOVE_ALIAS`
          (`redis.call('HDEL', aliases, alias); redis.call('HINCRBY', meta, 'alias_version', 1); return 1`),
          and delete dead `RedisBrokerCore.{_alias_cache,_alias_cache_version,_load_aliases_locked}`.
       5. Update `SM-REDIS-WRITE` in
          `docs/implementation/07-complexity-and-state-machine-map.md`,
          `extensions/simplebroker_redis/README.md`, and the four Redis test
          modules listed in `## Hidden Couplings`.
     - **Complete `RedisBrokerCore` error translation & fork/thread guards (`Findings 3, 10b, 10d, 10e`, `F6`, `F8`)**:
       - Add a small helper `def _call_redis(fn, *args, **kwargs)` /
         `try ... except redis.RedisError as exc: raise _translate_redis_error(exc) from exc`
         plus `self._check_fork_safety()` across all remaining bare
         `self._client.*` operations on `RedisBrokerCore` (including
         `_maybe_recover_stale_batches`, `_peek_rows`, `_zrange_pending`,
         `list_queues`, `get_queue_stats`, `get_overall_stats`,
         `count_claimed_messages`, `find_message_ids`, `has_pending_messages`,
         `latest_pending_timestamp`, `recover_stale_batches`,
         `get_alias_version`, `resolve_alias`, `has_alias`, `list_aliases`,
         `remove_alias`, and `get_meta`), preserving `[RUFF-SUP-020]` and
         `[RUFF-SUP-021]` and refreshing `bin/ruff_suppression_index.py --write`
         only if line numbers move.
       - Replace scalar `_active_generator_batch` /
         `_active_generator_batch_owner` (`core.py:228-229`) with
         `self._active_generator_batches: dict[int, Literal["claim", "move"]]`
         keyed by `threading.get_ident()`, guarded by `self._lock`, and cleared
         in `_check_fork_safety()`.
       - Validate `batch_size` via `resolve_generator_batch_size` across
         `RedisBrokerCore`: upfront explicit validation (`if batch_size is not None: resolve_generator_batch_size(batch_size, default=1)`)
         plus `default=int(effective_config["GENERATOR_BATCH_SIZE"])` in the
         `at_least_once` branches of `claim_generator` and `move_generator`
         (`[SB-API-4]`), and `default=PEEK_BATCH_SIZE` in `peek_generator`
         (`N1`); align the re-entrancy `RuntimeError` message with
         `simplebroker/db.py`.
       - Fix `RedisBackendPlugin.init_backend` (IPv6 `[host]`, `BACKEND_USER`,
         supplemental `BACKEND_PASSWORD`, redacted parse error) and wrap
         `client.ping()` in `validation.connect()` with
         `try ... except BaseException: client.close(); raise`.
- **Verification**:
  - `uv run --project extensions/simplebroker_pg pytest extensions/simplebroker_pg/tests`
  - `uv run --project extensions/simplebroker_redis pytest extensions/simplebroker_redis/tests`
  - `SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 uv run --frozen pytest tests/test_state_machine_policy.py`
  - `uv run --frozen python bin/ruff_suppression_index.py --check`

### Slice 4 — Documentation, Guides & Standalone Examples Alignment (Finding 10g)

- **Objective**: Align implementation docs, user guides, `README.md`,
  `CONTRIBUTING.md`, `03-agent-inventory.md`, `_dump.py`, `CHANGELOG.md`, and
  standalone examples with verified live contracts.
- **Files**:
  - `docs/implementation/07-complexity-and-state-machine-map.md`
  - `docs/implementation/03-agent-inventory.md`
  - `docs/guides/backends.md`
  - `docs/guides/python.md`
  - `README.md`
  - `CONTRIBUTING.md`
  - `simplebroker/_dump.py`
  - `examples/sqlite_connect.py`
  - `examples/dead_letter_queue.sh`
  - `CHANGELOG.md`
  - `tests/test_sqlite_connect_example.py` and `tests/test_shell_examples.py`
    (or existing example test modules in `tests/`)
- **TDD Steps (RED -> GREEN)**:
  1. **RED tests first**:
     - `examples/sqlite_connect.py`: `validate_database_path` succeeds on a
       valid SQLite database whose path contains `#`, `?`, `%`, and spaces
       (where raw `f"file:{file_path}?mode=ro"` currently misparses URI
       query/fragment delimiters).
     - `examples/dead_letter_queue.sh`: failing `process_message` on valid
       non-object JSON payloads (`"false"`, `"null"`, `"0"`, `"[1,2]"`,
       `"\"str\""`) and plain `"-n"` wraps each payload into
       `{"message": <raw>, "retry_count": 1}` without tripping `jq -e` exit `1`,
       `jq` scalar-indexing exit `5`, `null` payload loss, or `echo` flag
       parsing, while existing object envelopes (`{"message": "x", "retry_count": 1}`)
       increment `retry_count` to `2`.
  2. **GREEN updates**:
     - `docs/implementation/07-complexity-and-state-machine-map.md` line 421:
       update `(PostgreSQL: 60 s)` -> `(PostgreSQL: 30 s)`.
     - `docs/guides/backends.md` lines 57–60: accurately state that PostgreSQL
       leases pooled connections per operation (`lease_session_core_connection() -> False`)
       and reaps idle connections on quiet processes rather than retaining one
       checked-out connection per persistent reactor thread.
     - `README.md`: fix anchor `#backend-packaging-and-sidecar-boundary-sb-api-7`
       -> `#sidecar-sb-api-7` (line 91), `[SB-CLI-1]–[SB-CLI-5]` ->
       `[SB-CLI-1]–[SB-CLI-7]` (line 444), and remove stray `(` (line 644).
     - `docs/guides/python.md`: replace `"live offset paging"` with `"live keyset
       cursor paging"` (line 107) and `asyncio.get_event_loop()` inside
       `async def` with `asyncio.get_running_loop()` (lines 585, 595).
     - `CONTRIBUTING.md` line 50: update `uv` minimum version note to match
       `pyproject.toml` (`>=0.13.5`).
     - `docs/implementation/03-agent-inventory.md` lines 44–45: align Codex
       example flags with `skills/call-agent/SKILL.md` (`-c 'model_reasoning_effort="high"'`).
     - `simplebroker/_dump.py` line 278: update stale `backend-v7` docstring
       reference to current `BrokerConnection` contract (`backend-v9`).
     - `examples/sqlite_connect.py` (`validate_database_path`, line 466): build
       the read-only URI via `f"{file_path.resolve().as_uri()}?mode=ro"` (leaving
       `_release_lock` untouched per `F4`).
     - `examples/dead_letter_queue.sh` (lines 152–175): replace `echo "$raw_msg"` /
       `echo "$msg_data"` with `printf '%s\n'`, and replace both `jq -e .`
       checks (`lines 158 and 169`) with
       `printf '%s\n' "$raw_msg" | jq -e 'type == "object"' >/dev/null 2>&1` so
       only JSON objects enter `.retry_count` / `.message` envelope extraction
       and in-place `.retry_count` mutation (`F5`).
     - `CHANGELOG.md`: record user-visible fixes under `## [Unreleased]`.
- **Verification**:
  - `uv run --frozen python bin/release.py --check-example-types`
  - `uv run --frozen pytest -k "sqlite_connect or dead_letter"`
  - `./bin/check-doc-paths`

### Final Slice — Traceability Reconciliation, Full-Matrix Verification & Completed-Work Review

- **Objective**: Complete Strategy A traceability reconciliation (`_Verification_`
  citations in canonical specs), run full-matrix verification across core and
  both extensions, run the independent cross-family completed-work review, and
  align plan status.
- **Files**:
  - `docs/specs/10-cli.md` (`_Verification_` test citations)
  - `docs/specs/16-python-library-api.md` (`_Verification_` test citations)
  - `docs/specs/17-ops.md` (`_Verification_` test citations)
  - `docs/lessons.md` (only if execution exposes a genuinely new durable lesson
    not already covered)
  - `docs/plans/2026-11-10-deep-dive-review-remediation-plan.md`
  - `docs/plans/README.md`
- **Steps**:
  1. Add the newly landed firing test node IDs under `_Verification_` in
     `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`, and
     `docs/specs/17-ops.md`.
  2. Run every gate in `## Verification and Gates` below (including clean
     `git clone --no-local` verification and adversarial CLI acceptance probes).
  3. Run a Tier-1 cross-family completed-work review (`skills/call-agent/SKILL.md`,
     `review-loops-and-agent-bootstrap.md` §5), disposition all findings, and
     update `docs/plans/README.md` when closing the plan.

## Testing Plan

- **Anti-Mocking Posture (`hardening-plans.md` & `AGENTS.md`)**:
  - SQLite concurrency, `_stop_event` contention, `--cleanup` symlink containment,
    and CLI `--json`/`stderr` behavior are tested against **real on-disk SQLite
    databases, real OS symlinks, and real CLI invocations** — never mocked
    filesystem or mocked `BrokerCore` locks.
  - PostgreSQL `backend_options` rejection and connection-size locking are
    tested against both cold option validation entry points (`verify_env`,
    `init_backend`, `require_schema_name`, `create_runner`, `initialize_target`,
    `validate_target`, `cleanup_target`, `create_activity_waiter`, proving zero
    network contact on invalid options while `Queue` construction remains lazy)
    and a **real PostgreSQL instance** for end-to-end admission and `status()`.
  - Valkey/Redis multi-batch `vacuum()` (`2,500` claimed rows), Lua-embedded
    `PUBLISH` wakeup, `persisted_last_ts` return, monotonic `HINCRBY`
    `alias_version` across `add_alias`/`rename_queue`/`remove_alias`, and
    multi-thread generator isolation are tested against a **real Redis/Valkey
    server** (asserting observable Pub/Sub / waiter wakeup and resilience to
    injected Python `publish` / post-commit `hget` failures rather than pinning
    internal `INFO commandstats` counters). Controlled fault-injection wrappers
    around a real client are used *only* to synthesize transient
    `BusyLoadingError` / `ConnectionError` / `BUSY ` transport faults
    (`[SB-API-3]`) and deterministic mid-read alias-seqlock interleavings
    (`Finding 5`) that cannot be forced without deterministic scheduling hooks.
- **Adversarial CLI & Redaction Probes (`adversarial-acceptance-probes.md`)**:
  - Hostile filenames/queues containing `\x1b[31m`, `\r`, `\n`, `\x00`, C1 `\x9b`,
    and lone UTF-16 surrogates (`\udcff`) in both plain and `--json` modes.
  - Relative symlink chains (`a.db -> sub/b.db -> ../../outside/victim.db`),
    dangling symlinks, and self-referential symlink loops (`loop.db -> loop.db`)
    under `--cleanup`.
  - URLs with `@` in query/fragment (`localhost:5432/mydb?application_name=worker@prod`),
    numeric-prefix malformed passwords (`user:1234/secret@host/db`,
    `user:1234?secret@host/db`, `user:5432/secret?k=v@host/db`), combined `@` +
    `/` passwords (`user:p@ss/word@host/db`), IPv6 `[::1]`, and `sslpassword=`.

## Verification and Gates

Every command below must exit `0` before completion is claimed:

1. **Governance & Doc Gates (including clean-clone proof)**:
   - `./bin/coalesce-check && ./bin/coalesce-check --self-test`
   - `./bin/check-doc-paths`
   - `./bin/check-plan-context && ./bin/check-plan-context --self-test`
   - `python3 ./bin/check-dom15-fixtures`
   - Clean clone check: `git clone --no-local . "$TMP_CLONE"` -> `./bin/coalesce-check`
2. **Lint, Format, Complexity & Suppression Registry**:
   - `uv run --frozen ruff check .`
   - `uv run --frozen ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests`
   - `uv run --frozen python bin/ruff_suppression_index.py --check`
3. **Static Type Checking (Source, Tests, Extensions & Examples)**:
   - Full `mypy` source, core-test, and extension cohorts matching `.github/workflows/test.yml`
   - `uv run --frozen python bin/release.py --check-example-types`
4. **Core, State-Machine, Cross-Thread & Extension Test Suites**:
   - `uv run --frozen pytest`
   - `SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 uv run --frozen pytest tests/test_state_machine_policy.py`
   - `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run --frozen pytest tests/test_cross_thread_generator_probe.py`
   - `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run --frozen ./bin/pytest-pg --fast -n0 -q extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py`
   - `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run --frozen ./bin/pytest-redis --fast -n0 -q extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py`
   - `./bin/pytest-pg`
   - `./bin/pytest-redis`

## Out of Scope

- Coalescing/compacting the 61 entries in `docs/lessons.md` or pruning
  `docs/coalescing.md` (requires explicit coalescing sweep authorization per
  `AGENTS.md`).
- Bumping `BACKEND_API_VERSION` beyond `9` or modifying SQL schema v6 tables/indexes
  (`[ALT-IMPL09-001]`, `[ALT-IMPL09-002]`).
- Splitting `simplebroker/db.py`, `simplebroker/cli.py`, or `simplebroker/commands.py`
  by line count (`[REV-THEORY-001]`), or overturning approved `[RUFF-SUP-*]`
  exceptions (`[DOM-10.1]`).
- Adding broker-owned visibility leases or background reaper daemons
  (`[THEORY-2]`, `[THEORY-6]`, `[ALT-THEORY-001]`).

## Deviation Log

- None — implemented across `Slice 1` through `Final Slice` as scoped in `v4`.

## Revision Log

- `2026-11-10` (v1): Initial Class 5+P remediation plan drafted covering all
  verified findings (`Findings 1–10g`) and owner decisions Q1 (`--cleanup`
  relative containment) and Q2 (atomic Lua `PUBLISH` + `last_ts` return).
- `2026-11-10` (v2 — post-Codex Round 1 review): Addressed all eight Round-1
  external review findings (`F1`–`F8`):
  - `F1`: Replaced broken `_run_thread is not None` guard with
    `_lifecycle_state_locked()` under `_stop_lock`, rejecting only `RUN_OWNED`
    or a live `_thread` while preserving `IDLE`, `RELEASED`, and `[SB-API-6]`
    `STOP_OWNED` stop-before-start semantics.
  - `F2`: Replaced port-range heuristic with conservative fail-closed RFC-3986
    post-path query/fragment rule, added explicit tests for numeric-prefix and
    multi-delimiter malformed passwords (`user:1234/secret@host/db`,
    `user:p@ss/word@host/db`, `user:1234?secret@host/db`,
    `user:5432/secret?k=v@host/db`), and surfaced the residual syntactic overlap
    as `HR-1` under `## Human-Review Decision Points`.
  - `F3`: Preserved `batch_size: int | None = None` default resolution via
    `resolve_generator_batch_size`.
  - `F4`: Removed the false-premise `examples/sqlite_connect.py` `_release_lock`
    unlink change (moving it to `## No-Action Register`) while keeping the
    `validate_database_path` URI encoding fix.
  - `F5`: Required `jq -e 'type == "object"'` at both envelope branches in
    `examples/dead_letter_queue.sh`.
  - `F6`: Fixed all path/symbol/signature references (`09-storage-schema-and-claim-lifecycle.md`,
    `11-delivery.md`, `13-message-identity.md`, `tests/test_aliases_db.py`,
    `github_api_get` / `RuntimeError`, module-level `verify_env(..., toml_options=...)`,
    `_translate_redis_error(exc)` preserving `TRANSPORT-AMBIGUOUS`,
    `get_overall_stats`, `RedisBrokerCore` vs `RedisBackendPlugin` methods,
    `_active_generator_batch_owner`, `validate_database_path`, and stack-scoped
    `_stop_event` without adding `get_stop_event()` to `BrokerConnection` v9).
  - `F7`: Included `ADD_ALIAS` and `RENAME_QUEUE` alongside `REMOVE_ALIAS` in
    converting Redis `alias_version` updates from wall-clock `HSET` to monotonic
    `HINCRBY`.
  - `F8`: Replaced blanket `C901 <= 10` invariant with Ruff + suppression-registry
    governance preserving approved `[RUFF-SUP-020]` and `[RUFF-SUP-021]`.
- `2026-11-10` (v3 — post-Codex Round 2 scoped verification): Addressed both
  remaining Round-2 items (`N1` and `N2`):
  - `N1` (on `F3`): Made `default: int` a required keyword parameter on
    `resolve_generator_batch_size(batch_size: int | None, *, default: int) -> int`,
    preserving `effective_config["GENERATOR_BATCH_SIZE"]` for `at_least_once`
    `claim_generator`/`move_generator` (`[SB-API-4]`, `tests/test_connection_config.py`)
    and `PEEK_BATCH_SIZE` (`1000`) for `peek_generator` across SQL and Redis
    while validating explicit non-`None` `batch_size` in both `exactly_once`
    and `at_least_once` modes without resolving `config` in `exactly_once`.
  - `N2` (on `F6`): Removed references to nonexistent
    `PostgresBackendPlugin.validate_backend_options` and removed `simplebroker/sbqueue.py`
    from PostgreSQL option edits (preserving lazy non-Redis `Queue` construction
    per `[SB-API-2]`, `tests/test_backend_plugin_resolution.py:209-228`, and
    core-only `tests/test_queue_api_additions.py:249-256`), targeting the two
    live PostgreSQL option validation owners (`require_schema_name` in
    `extensions/simplebroker_pg/simplebroker_pg/validation.py` and module-level
    `verify_env` in `extensions/simplebroker_pg/simplebroker_pg/plugin.py`).
- `2026-11-10` (v4 — owner scoping & execution sign-off): Placed `--cleanup`
  containment at the front of Slice 2, adopted the conservative `HR-1`
  `sslpassword` redaction rule (retaining fail-closed masking through the last
  `@` and honoring `--quiet` on CLI `.broker.toml` commentary), scoped Redis
  activity/timestamp changes to remove the unused wildcard publish, embed
  per-queue `PUBLISH` in the six mutating Lua scripts, return committed
  `last_ts` strings from `INSERT_MESSAGES` / `BROADCAST_MESSAGE`, and assert
  observable wakeup + fault behavior rather than `INFO commandstats`, and
  resolved Finding `10c` by publishing the in-transaction `meta.last_ts` floor
  directly via `_publish_committed_floor` instead of issuing a post-commit
  `refresh_last_timestamp()` read.
- `2026-11-10` (v5 — post-implementation Codex completed-work review): Resolved
  all three Round-1 completed-work review findings (`F1`–`F3` in
  `extensions/simplebroker_redis/simplebroker_redis/plugin.py`,
  `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`, and
  `CHANGELOG.md`) and verified **`PASS`** in Round 2.
- `2026-11-10` (v6 — pre-push review remediation + Codex review loop): Remediated
  all owner pre-push review findings (`P1` lease-keyed Redis generator guard +
  non-vacuous finalization probe assertion; `P2` token-keyed out-of-order
  `stop_event` frames in `_ProcessBrokerSession` and `DBConnection`; `P2`
  post-loop `redis.pcall('PUBLISH', ...)` across all 7 Lua publish sites; `P2`
  `CHANGELOG.md` `[Unreleased]` accuracy; `P2` `bin/coalesce-check`
  `HEAD`/`origin/main` reachability and `bin/check-plan-context` house-style
  sibling-plan resolution; plus `require_green_workflows.py` dropped-connection
  / truncated-body retry, `_WatcherThread` immediate post-crash restart,
  `BROKER_GENERATOR_BATCH_SIZE` positive config validation, `get_alias_version`
  `-1` sentinel guard, discriminating Lua/stop-event tests, and SQLite/PG/Redis
  finalization probe gate list), resolved Codex Round-1 findings `F1`–`F4`, and
  verified **`PASS`** in Round 2.

## Independent Review Loop

- Author family: Google (Gemini / Antigravity)
- Reviewer family: OpenAI (Codex)
- Review invocation: `codex exec -s read-only -C "$(git rev-parse --show-toplevel)" "$PROMPT" --json -c 'model_reasoning_effort="high"'`

### Round 1 — Whole-Plan Review (`2026-11-10`, Verdict: `BLOCKED` -> `F1`–`F8` addressed in v2)

| ID | Severity | Location | Summary | Disposition |
| :--- | :---: | :--- | :--- | :--- |
| `F1` | `[P1]` | Slice 2, `BaseWatcher.run_in_thread` | `_run_thread` initializes to `_WatcherLifecycleState.IDLE`, so `_run_thread is not None` would reject fresh watchers. | **Accepted — fixed in v2 (`PASS` in Round 2).** Switched to `_lifecycle_state_locked()` under `_stop_lock`, rejecting `RUN_OWNED` or a live `_thread` while preserving `IDLE`, `RELEASED`, and `[SB-API-6]` `STOP_OWNED` stop-before-start behavior (note: `STOP_OWNED` intentionally allows a no-op thread exit rather than raising `RuntimeError` so a racing `stop()` before `with watcher:` obeys `[SB-API-6]` lines 822–825). |
| `F2` | `[P1]` | Slice 2, `_targets.py` URL redaction | Port heuristic exposed `postgresql://user:1234/secret@host/db` and `postgresql://user:p@ss/word@host/db`; overlap between passwordless `host:port` URLs with `@` in query and malformed numeric-prefix passwords requires an explicit policy and human review. | **Accepted — fixed in v2 + escalated as `HR-1` (`PASS` in Round 2).** Replaced heuristic with fail-closed RFC-3986 post-path query/fragment rule (requiring valid `urlsplit`/`port`, whitespace-free input, no `@` in `path`, `/` path before `?`/`#` when `:` is in `hostport`, and no `/` after the last `@`, running raw-last-`@` masking without partial truncation otherwise); added explicit RED cases and `## Human-Review Decision Points` (`HR-1`). |
| `F3` | `[P1]` | Slice 2, generator `batch_size` validator | Rejecting all non-`int` inputs at entry would reject `batch_size=None` default used by `Queue.read_generator()`. | **Accepted — refined in v2 & v3 (`N1`).** Required explicit `default: int` on `resolve_generator_batch_size`, preserving `effective_config["GENERATOR_BATCH_SIZE"]` for `claim_generator`/`move_generator` (`[SB-API-4]`) and `PEEK_BATCH_SIZE` for `peek_generator` across SQL and Redis. |
| `F4` | `[P2]` | Slice 4, `examples/sqlite_connect.py` | `_release_lock` only unlinks when `not HAS_FCNTL and not HAS_MSVCRT` (`O_CREAT | O_EXCL` fallback); POSIX already keeps the inode and removing the fallback unlink wedges fallback locks. | **Accepted — fixed in v2 (`PASS` in Round 2).** Removed the `_release_lock` change from Finding `10g` / Slice 4 and recorded it in `## No-Action Register`, retaining only the `validate_database_path` URI fix. |
| `F5` | `[P2]` | Slice 4, `examples/dead_letter_queue.sh` | `jq -e 'type'` admits scalars (`false`, `null`, `0`, strings), causing `.retry_count` indexing exit `5` or overwriting `null` payloads. | **Accepted — fixed in v2 (`PASS` in Round 2).** Mandated `jq -e 'type == "object"'` at both lines 158 and 169 with RED tests for scalar/array/object payloads. |
| `F6` | `[P2]` | `Source Documents` & Slices 1–4 | Fixed 11 path/symbol/signature mismatches (`09-storage-schema-and-claim-lifecycle.md`, `11-delivery.md`, `13-message-identity.md`, `test_aliases_db.py`, `github_api_get` / `RuntimeError`, module-level `verify_env(..., toml_options=...)`, `_translate_redis_error(exc)` + `TRANSPORT-AMBIGUOUS`, `get_overall_stats`, `RedisBackendPlugin` vs `RedisBrokerCore` methods, `_active_generator_batch_owner`, `validate_database_path`, and stack-scoped `_stop_event` without `core.get_stop_event()`). | **Accepted — refined in v2 & v3 (`N2`).** All paths, signatures, and existing PostgreSQL validation owners (`require_schema_name` and `verify_env`) verified against the live tree. |
| `F7` | `[P3]` | Invariant 6 & Slice 3 Redis aliases | `ADD_ALIAS` and `RENAME_QUEUE` currently `HSET` wall-clock timestamps rather than `HINCRBY`. | **Accepted — fixed in v2 (`PASS` in Round 2).** Explicitly updated `ADD_ALIAS`, `RENAME_QUEUE`, and new `REMOVE_ALIAS` to use `HINCRBY meta alias_version 1` and drop timestamp `ARGV`s. |
| `F8` | `[P2]` | Invariant 8 | Blanket raw `C901 <= 10` contradicted approved `[RUFF-SUP-020]` and `[RUFF-SUP-021]` exceptions on edited Redis methods. | **Accepted — fixed in v2 (`PASS` in Round 2).** Updated Invariant 8 and Slice 3 to preserve approved `[RUFF-SUP-*]` exceptions and enforce `ruff check` + `ruff_suppression_index.py --check`. |

### Round 2 — Scoped Re-Review (`2026-11-10`, Verdict: `FAIL` on `N1` & `N2`; `PASS` on `F1`, `F2`, `F4`, `F5`, `F7`, `F8` -> `N1` & `N2` addressed in v3)

| ID | Severity | Location | Summary | Disposition |
| :--- | :---: | :--- | :--- | :--- |
| `N1` | `[P2]` | `F3` — Slice 2 & Slice 3 generator `batch_size` defaults | Universal `DEFAULT_BATCH_SIZE` fallback on `resolve_generator_batch_size` would override `effective_config["GENERATOR_BATCH_SIZE"]` (`[SB-API-4]`, breaking `tests/test_connection_config.py:424` where `GENERATOR_BATCH_SIZE=2` commits after 2 yields) and `PEEK_BATCH_SIZE=1000` on `peek_generator`. | **Accepted — fixed in v3 (`PASS` in Round 3).** Made `default: int` a required keyword argument on `resolve_generator_batch_size(batch_size: int | None, *, default: int) -> int`; `claim_generator` and `move_generator` validate explicit non-`None` `batch_size` upfront (`exactly_once` and `at_least_once`, without resolving `config` in `exactly_once`) and pass `default=int(effective_config["GENERATOR_BATCH_SIZE"])` in `at_least_once`, while `peek_generator` passes `default=PEEK_BATCH_SIZE` across SQL and Redis. |
| `N2` | `[P2]` | `F6` — Slice 3 PostgreSQL option validation owners | `PostgresBackendPlugin.validate_backend_options` does not exist in `extensions/simplebroker_pg/simplebroker_pg/plugin.py` (line 527 is `init_backend`), and non-Redis `Queue(...)` construction must stay lazy (`tests/test_backend_plugin_resolution.py:209-228` and core-only `tests/test_queue_api_additions.py:249-256`). | **Accepted — fixed in v3 (`PASS` in Round 3).** Removed all references to nonexistent `validate_backend_options` and kept `sbqueue._canonicalize_queue_target` untouched/lazy for `"postgres"`, targeting the two existing PostgreSQL option validation owners (`require_schema_name` in `validation.py:53` and module-level `verify_env` in `plugin.py:288`) before any DB connection is opened. |

### Round 3 — Scoped Re-Review (`2026-11-10`, Verdict: `PASS`)

- **Scope**: Scoped verification of `N1` (`F3` generator `batch_size` default resolution) and `N2` (`F6` PostgreSQL `backend_options` validation owners) in `v3` against baseline `HEAD` `93d2aec`.
- **Result**: **`PASS`** (`N1: PASS`, `N2: PASS`; zero new defects or incomplete fixes).
  - `N1`: Confirmed required `default: int` preserves configured `GENERATOR_BATCH_SIZE` for transactional `claim_generator`/`move_generator` (`[SB-API-4]`, `tests/test_connection_config.py:424`) and `PEEK_BATCH_SIZE=1000` for `peek_generator` across SQL and Redis while rejecting invalid explicit values upfront without resolving `config` in `exactly_once`.
  - `N2`: Confirmed `require_schema_name` (`extensions/simplebroker_pg/simplebroker_pg/validation.py:53`) and module-level `verify_env` (`extensions/simplebroker_pg/simplebroker_pg/plugin.py:288`) are the exact existing validation owners covering target-based runner, lifecycle, waiter, and configuration resolution before database connections open, while leaving PostgreSQL `Queue` canonicalization lazy (`tests/test_backend_plugin_resolution.py:209-228` and core-only CI).

### Completed-Work Review — Round 1 (`2026-11-10`, Verdict: `blocker: F1, F2 — BLOCKED` -> `F1`–`F3` addressed)

- **Unit under review**: Full 63-file uncommitted implementation delta (`Slice 1` through `Final Slice`) against baseline `HEAD` `93d2aec` (`thread_id=01a1c821-5b30-7410-9e66-69fa8aacb0bd`).

| ID | Severity | Location | Summary | Disposition |
| :--- | :---: | :--- | :--- | :--- |
| `F1` | `[P2]` | `extensions/simplebroker_redis/simplebroker_redis/plugin.py:96` | `parsed.scheme in ("redis", "rediss") and not parsed.hostname` rejected previously accepted hostless and query-host URLs (`redis://`, `redis:///2`, `redis://?host=localhost&db=2`) supported by `redis.connection.parse_url`. | **Accepted — fixed (`PASS` in Round 2).** Removed the synthetic `not parsed.hostname` check while keeping `urlsplit` + `parsed.port` + `parse_redis_url` validation, and added regression assertions for `redis://`, `redis:///2`, `redis://?host=localhost&db=2`, and `unix:///tmp/redis.sock?db=1` (with and without `BROKER_BACKEND_PASSWORD` supplementation) in `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`. |
| `F2` | `[P2]` | `extensions/simplebroker_redis/simplebroker_redis/plugin.py:98` | Checking only `parsed.password is None` before supplementing `BROKER_BACKEND_PASSWORD` overwrote explicit query-parameter credentials (`redis://localhost/0?password=explicit`). | **Accepted — fixed (`PASS` in Round 2).** Checked `parsed.password is not None or parsed_options.get("password") is not None` (from `parse_redis_url`) before supplementing `BROKER_BACKEND_PASSWORD`, and added query-password precedence assertions across `redis://` and `unix://` URLs in `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`. |
| `F3` | `[P3]` | `CHANGELOG.md:22` | Changelog entry listed `get_alias_version` among mutators advancing `alias_version` and said `BrokerCore.insert_messages` ran a post-commit `refresh_last_timestamp()` rather than publishing the committed floor directly. | **Accepted — fixed (`PASS` in Round 2).** Updated `CHANGELOG.md` to state that `add_alias`, `remove_alias`, and alias-retargeting `rename_queue` advance `alias_version` monotonically while `get_alias_version` / `_aliases` verify cache stability, and that `BrokerCore.insert_messages` publishes the committed in-transaction `meta.last_ts` floor via `_publish_committed_floor` without a post-commit `refresh_last_timestamp()` read. |

### Completed-Work Review — Round 2 (`2026-11-10`, Verdict: `PASS`)

- **Scope**: Scoped verification of accepted findings `F1`, `F2`, and `F3` (`thread_id=01a1c829-135b-72f1-9a58-b9265ac49f8d`).
- **Result**: **`PASS`** (`F1`, `F2`, and `F3` verified against targeted tests, 48 URL/source/password combinations including `rediss://`, and the landed implementation; zero new defects).

### Pre-Push Remediation Review — Round 1 (`2026-11-10`, Verdict: `blocker: F1, F2` -> `F1`–`F4` addressed)

- **Unit under review**: 28-file pre-push remediation delta over `12ddb4d` in context of `93d2aec..HEAD` (`thread_id=01a1c888-6259-7151-9a92-bf96f29b5e34`).

| ID | Severity | Location | Summary | Disposition |
| :--- | :---: | :--- | :--- | :--- |
| `F1` | `[P2]` | `bin/check-plan-context:333`, `tests/test_doc_gates.py` | Exemption for `docs/plans/<retired-plan>.md` checked membership in `retired_plan_names` without checking `not target.exists() and not target.is_symlink()`, so an existing escaping symlink at that path would bypass `_resolves_within_root`. | **Accepted — fixed (`PASS` in Round 2).** Added `not target.exists() and not target.is_symlink()` to the retired slash-path exemption in `bin/check-plan-context` and added regression assertion `# 3b` in `tests/test_doc_gates.py` proving missing retired `docs/plans/...` passes while an escaping symlink fails. |
| `F2` | `[P2]` | `tests/test_watcher_cleanup.py:556` | `CrashOnceWatcher` could enter `threading.excepthook` before the caller completed `first_thread = watcher.run_in_thread()`, risking `NameError` under thread scheduling jitter. | **Accepted — fixed (`PASS` in Round 2).** Initialized `first_thread: threading.Thread | None = None` and gated `CrashOnceWatcher._drain_queue` on a `first_thread_stored` `threading.Event` set immediately after `watcher.run_in_thread()` returns. |
| `F3` | `[P3]` | `tests/test_doc_gates.py:495` | `# 4` real-plan probe called `.replace("completed — class 4", ...)` on a `retired-pending` row (a silent no-op checking 0 active plans). | **Accepted — fixed (`PASS` in Round 2).** Replaced with `re.subn(..., count=1, flags=re.MULTILINE)`, asserted `count == 1`, and asserted `"1 in-flight plan source declarations resolve" in res_real_plan.stdout`. |
| `F4` | `[P3]` | `CHANGELOG.md:36` | Changelog entry conflated explicit `batch_size` non-`int`/`bool` rejection with `BROKER_GENERATOR_BATCH_SIZE` config validation (which uses standard `[SB-API-2]` integer coercion before enforcing `>= 1`). | **Accepted — fixed (`PASS` in Round 2).** Updated `CHANGELOG.md` to distinguish explicit `batch_size` `ValueError` validation from `BROKER_GENERATOR_BATCH_SIZE` positive-after-coercion `InvalidConfigError` validation. |

### Pre-Push Remediation Review — Round 2 (`2026-11-10`, Verdict: `PASS`)

- **Scope**: Scoped verification of accepted findings `F1`–`F4` (`thread_id=01a1c890-6ad8-7b90-936f-59d4b292fe37`).
- **Result**: **`PASS`** (`F1`–`F4` verified against live `bin/check-plan-context`, `[SB-API-2]`, and 13 read-only validation probes; zero new defects).
