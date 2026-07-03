# Checkpoint/Move Caveat and Lock Hardening Plan

Date: 2026-07-03

Status: implemented

Owner: SimpleBroker

Review: iteratively reviewed by Codex until implementable; see the Review
Log at the bottom of this file for rounds and finding counts.

## Purpose

Address the four highest-priority findings from the 2026-07-03 multi-factor
evaluation (run against v4.10.0, commit `5a59d52`), plus one CI defect
discovered while validating this plan:

1. **[HIGH, confirmed by reproduction]** Messages moved into a queue are
   permanently skipped by timestamp-checkpoint consumers, because moves
   preserve the original timestamp by design while checkpoints filter on
   `ts > last_seen`. Affected consumers: `peek --after`, `read --after`,
   peek-mode watchers, consume-mode watchers started with `after_timestamp`,
   and any hand-rolled `ts > last_seen` filter. This is data-loss-shaped for
   the documented "Checkpoint-based Processing" pattern. This plan documents
   the caveat prominently, pins the semantics with characterization tests,
   and records a consumer audit. It does NOT change move semantics.
2. **[MEDIUM, confirmed]** `_phaselock.py` derives its lock/status sidecar
   paths with `Path.with_suffix` (`_phaselock.py:506-507`), so same-stem
   targets (`mydb.db`, `mydb.backup`) collide on `mydb.lock`/`mydb.status`.
   On xattr-less filesystems, shared status markers can cause a sibling file
   to inherit "connection"/"schema" completion markers — skipping WAL
   enablement or schema creation. The vacuum lock file has the same
   `with_suffix(".vacuum.lock")` collision pattern.
3. **[MEDIUM, confirmed]** The SQLite vacuum lockfile
   (`_backends/sqlite/maintenance.py` ~:148-190) uses `O_CREAT|O_EXCL` +
   PID + mtime staleness — the pattern `_phaselock.py`'s own docstring
   forbids ("file existence is never considered lock ownership"), with a
   TOCTOU in stale-lock removal and a silent up-to-
   `BROKER_VACUUM_LOCK_TIMEOUT` vacuum outage after SIGKILL. The
   kernel-released `AdvisoryFileLock` in the same package is the right tool.
4. **[MEDIUM-LOW, confirmed]** Fork-safety guards (`_check_fork_safety`;
   22 call sites in `db.py`) are missing from five public entry points —
   `generate_timestamp` (db.py:984), `get_cached_last_timestamp` (:999),
   `refresh_last_timestamp` (:1005), `sidecar` (:1012), `get_data_version`
   (:2776) — and the runner's fork fallback **closes** the inherited SQLite
   connection in the child (`_runner.py:211-219`), which SQLite documents
   as unsafe (close implies rollback and can touch the shared WAL-index
   while the parent is mid-transaction).
5. **[NEW, CI, confirmed]** The "phaselock fallback-path gate" CI step sets
   `_ENABLE_PHASELOCK_XATTRS: "0"` (`.github/workflows/test.yml:58`) but the
   code reads `PHASELOCK_ENABLE_XATTRS` (`_phaselock.py:66`). The gate has
   been a no-op: the xattr-less fallback path is not actually exercised in
   CI.

Plus one surface-contract cleanup:

6. **[LOW-MEDIUM]** `simplebroker.commands.__all__` (commands.py:1237-1252)
   is **incomplete**: it exports 13 `cmd_*` functions plus
   `parse_exact_message_id` but omits `cmd_alias_list`, `cmd_alias_add`,
   `cmd_alias_remove`, `cmd_rename`, `cmd_dump`, `cmd_load`. The module is
   consumed by weft (`weft/commands/queue.py:24`) but is not documented as
   public embedding surface. Separately, both first-party extensions import
   `DatabaseError` from `simplebroker._exceptions` because `ext.py` does not
   re-export it.

This plan assumes the implementer is a skilled Python developer but new to
this repository. Follow the tasks in order. Use red-green TDD where behavior
changes; use characterization tests (green-first, pinning current behavior)
where the task is documentation. Keep changes surgical. Do not redesign
watcher delivery, move semantics, or the phase-lock protocol.

## Relationship to Other Plans

- `2026-07-02-evaluation-fixes-plan.md`: shipped in v4.10.0 (commit
  `f636be9` et al.). It fixed a *different* checkpoint defect — the
  write-side timestamp-allocation race. Task 1 here concerns moves, a
  different mechanism (old-ts rows appearing in a new queue). Do not touch
  the write path.
- `2026-07-02-watch-after-and-pg-rename-lock-plan.md`: status says
  `proposed`, but the watcher change **appears already implemented at
  HEAD** — `watcher.py` now enforces `after_timestamp` in consume mode via
  `_consume_one_message` (:1562) / `_consume_all_messages` (:1587). Task 0
  verifies this and, if confirmed, flips that plan's status rather than
  re-doing its work. Consequence for Task 1: consume mode is an escape
  hatch from the move-skip **only when started without `after_timestamp`**.
- `2026-07-02` extension-contract documentation work locked "documentation
  only, no exports are added" for the extension seam. Task 5's
  `DatabaseError` export **deliberately overrides** that decision, narrowly:
  `DatabaseError` is an exception class *consumers must catch*, needed by
  both first-party extensions and any embedder handling failures — it is
  error-handling surface, not backend-author surface. The override is
  limited to exception classes both extensions already import; nothing else
  is promoted (see Locked Decisions).
- The 2026-07-03 evaluation also produced a **weft-side** recommendation
  (audit weft/taut consumers for move-skip exposure). Task 1.4 is an
  OPTIONAL, read-only, cross-repo step: skip it if the sibling repos are
  not present; any code changes belong in those repos.

## Locked Decisions

Do not reopen these unless a failing test proves the plan cannot work.

### Conventions for this plan

- **Commit messages:** plain imperative sentences (matching the dominant
  style in `git log`: "Make redis use shared validators", "Add
  --before/--after"), NOT conventional-commit prefixes. The exact message is
  given per task.
- **Test invocation:** `uv run pytest ...` is authoritative (matches the
  2026-07-02 plans). `./.venv/bin/python -m pytest` is an acceptable local
  equivalent if `uv` is unavailable.
- New test modules that exercise only the public Queue/watcher API must
  carry `pytestmark = [pytest.mark.shared]` **explicitly** — modules that do
  not use `run_cli` default to sqlite-only (tests/conftest.py ~:874).
  Modules touching phaselock/vacuum/fork internals stay unmarked
  (sqlite-only) by design.

### Move/checkpoint semantics (Task 1)

- **Moves preserve message timestamps. This stays.** Stable IDs are a
  documented design property; weft's requeue path depends on claim-order
  semantics that are unaffected. Do NOT add a re-timestamping option to
  `move` in this plan — separate product decision, separate plan.
- The fix here is: (a) a prominent README caveat, (b) docstring caveats at
  the checkpoint-filter code, (c) characterization tests pinning the skip so
  any future change is deliberate, (d) an optional recorded consumer audit.
- The caveat must name ALL affected consumer shapes: `peek --after`,
  `read --after`, peek-mode watchers, consume-mode watchers WITH
  `after_timestamp`, and hand-rolled `ts > last_seen` filters. The safe
  patterns are: consume mode with NO timestamp filter (ordering is by id,
  claiming whatever is unclaimed), or periodic full rescans from
  `--after 0`. Do not describe consume mode as a general escape hatch.

### Phase-lock and vacuum-lock paths (Tasks 2, 3)

- Sidecar paths are built by **appending** to the full target name
  (`Path(str(target) + suffix)`), never by `with_suffix`. `mydb.db` →
  `mydb.db.lock`; `mydb.backup` → `mydb.backup.lock`. Same rule for the
  status file and the vacuum lock.
- **Mixed-version window is accepted and must be verified, not assumed**:
  during an upgrade, an old process may hold `mydb.lock` while a new one
  holds `mydb.db.lock`, so cross-version setup serialization is lost. Task
  2.4 adds a test running two concurrent setups against one database with
  the two different lock paths, asserting the DB ends in WAL mode with an
  intact schema (setup phases are idempotent and SQLite serializes the
  underlying DDL — prove it).
- **Old sidecar files are orphaned forever.** After the rename, existing
  `mydb.lock`/`mydb.status` files are never read, never cleaned up, and
  never migrated (tiny files; a migration shim is not worth the risk).
  Status markers move once, so setup re-runs once per database after
  upgrade (idempotent). Tests must not assert the absence of old-style
  files. Record in CHANGELOG.
- The vacuum lock keeps its **non-blocking skip semantics**: if another
  process holds the lock, skip vacuum silently. Replace only the mechanism
  (flock via `AdvisoryFileLock`), not the policy. The lock file is
  **never unlinked** (phaselock doctrine: the flock is ownership, the file
  is permanent) — this is a deliberate behavior change; existing tests that
  assert the lockfile disappears are updated in Task 3.3.
- `BROKER_VACUUM_LOCK_TIMEOUT` becomes inert: keep parsing it (no config
  break), stop using it for staleness, update its description in
  `_constants.py` (it currently claims it prevents stuck vacuums), mark
  deprecated in the CHANGELOG.

### Fork handling (Task 4)

- The five unguarded methods get the same `self._check_fork_safety()`
  call, first line, matching the 22 existing call sites in db.py.
- The runner's fork fallback must **never close** an inherited connection in
  the child — and must never let one be GC-finalized either. Abandon
  EVERYTHING inherited: before clearing `_thread_local` and
  `_all_connections` (`_runner.py:~226`), append the current
  `_thread_local.conn` AND every connection tracked in `_all_connections`
  to a module-level `_ABANDONED_FORK_CONNECTIONS: list[Any]` — clearing
  `_all_connections` without this can drop the last child-side reference to
  an inherited connection from another thread-local generation, and CPython
  finalization closes it, reintroducing the exact hazard being fixed.
  Deliberate, bounded leak; comment cites SQLite's fork guidance ("do not
  use a connection across a fork, including close"). Add the `Any` import
  to `_runner.py` if absent.
- Test evidence is **child-side and behavioral**, communicated over a pipe
  or exit code (existing fork-test style): the abandoned object is present
  in `_ABANDONED_FORK_CONNECTIONS`, and reading a cheap attribute
  (`conn.total_changes`) does NOT raise `sqlite3.ProgrammingError` (closed
  connections raise it). Do NOT try to spy on `Connection.close` — it is a
  C-level method and not reliably patchable. A parent-side "still works"
  check proves nothing (child close does not close the parent's fd) and
  must not be presented as evidence.
- Cover the public path too: `Queue.generate_timestamp()` delegates to the
  core — the parametrized guard test includes one case through `Queue`.
- Do not use `os.register_at_fork` (import-time side effects; fires for
  unrelated forks). The lazy pid check in `get_connection` remains the
  pattern.

### Public-surface blessing (Task 5)

- `simplebroker.commands` becomes documented public embedding surface **as
  a module** (import path `simplebroker.commands`): **replace** the existing
  incomplete `__all__` (commands.py:1237-1252) with all 19 `cmd_*` functions
  plus the already-exported `parse_exact_message_id` (20 names), extend the
  module docstring with the stability promise, and add a README "Command
  layer" subsection. Do NOT re-export from the package `__init__`.
- `ext.py` re-exports `DatabaseError` only. The Task 5.2 audit of extension
  private imports is **inventory-only**: both extensions intentionally
  import many private modules under the documented lockstep-pin contract
  (`ext.py:7-27`); do not promote anything beyond exception classes that
  BOTH extensions import. Expected outcome: **no new export beyond
  `DatabaseError`** — the other shared exception imports
  (e.g. `OperationalError`, imported by both extensions) are already in
  `ext.__all__`; the inventory should confirm that rather than re-promote
  them.

## Repository Primer

### Runtime files

| File | Role in this plan |
|---|---|
| `simplebroker/_phaselock.py` | Sidecar path derivation (:506-507); `AdvisoryFileLock` (:463); `PHASELOCK_ENABLE_XATTRS` (:66); exceptions (e.g. `PhaseLockTimeout` :80 — enumerate the actual classes before Task 3) |
| `.github/workflows/test.yml` | Broken fallback-gate env var (:58) |
| `simplebroker/_backends/sqlite/maintenance.py` | `vacuum()` lockfile (~:148-190) |
| `simplebroker/_runner.py` | Fork fallback in `get_connection` (~:205-235) |
| `simplebroker/db.py` | `_check_fork_safety` pattern (22 call sites); the five unguarded methods |
| `simplebroker/watcher.py` | Peek checkpoint advance (~:1541), drain filter (~:1550), pending pre-check (~:1456); consume-mode after_timestamp paths (`_consume_one_message` :1562, `_consume_all_messages` :1587); consume-mode warning block (~:1334-1356) is the doc-style reference |
| `simplebroker/_sql/sqlite.py` | `RETRIEVE_MOVE` preserving `ts` (~:177-187) — read, do not modify |
| `simplebroker/commands.py` | 19 `cmd_*` functions; incomplete `__all__` at :1237 |
| `simplebroker/ext.py` | `__all__` exceptions group (~:78-87) |
| `simplebroker/_constants.py` | `BROKER_VACUUM_LOCK_TIMEOUT` description |

### Existing tests this plan touches (enumerated up front)

- `tests/test_phaselock.py` — extends; some literal-path assertions update
  to the new naming.
- `tests/test_runner_validation.py`, `tests/test_runner_error_handling.py`,
  `tests/test_sqlite_setup_contention.py`, `tests/test_queue_config_defaults.py`
  — may assert literal `broker.lock`/`broker.status` paths; sweep with
  `grep -rn '\.lock\|\.status' tests/` and update literals to the new
  naming (Task 2.3).
- `tests/test_edge_cases.py::test_vacuum_lock_cleanup_after_crash` and
  `::test_vacuum_lock_timeout_environment_variable` — encode the OLD
  stale-mtime behavior and the old `with_suffix` path; deliberately
  rewritten in Task 3.3 (crash-recovery semantics are now "flock released by
  kernel", not "mtime staleness"). Any other
  `with_suffix(".vacuum.lock")` literals in tests updated to the helper.
- `tests/test_move_by_id.py::test_move_by_id_preserves_timestamp` — already
  pins ts preservation; Task 1 references it instead of duplicating.
- `tests/test_ext_imports.py` — asserts exact `ext.__all__`; extended in
  Task 5 (do not create a parallel surface test for ext).
- `tests/test_fork_safety.py`, `tests/test_sidecar.py` — extended in Task 4;
  check no existing test forks-then-uses a newly guarded method expecting
  success (if one does, it was depending on the hazard: update it to expect
  `RuntimeError`, with a comment).

### Toolchain

- `cd /Users/van/Developer/simplebroker`; `uv run pytest -q` (default run
  excludes slow tests; none of this plan's tests are slow).
- Lint/type: `uv run ruff check simplebroker && uv run ruff format --check
  simplebroker && uv run mypy simplebroker`.
- Extension suites (Task 5 touches `ext.py`): `bin/pytest-pg` and
  `bin/pytest-redis` need Docker; if unavailable, run
  `uv run mypy extensions/simplebroker_pg extensions/simplebroker_redis`
  and note the skipped container runs in Completion Notes.
- CHANGELOG: Keep-a-Changelog. Add entries under `## [Unreleased]` (create
  atop `CHANGELOG.md` if absent) per task.

### Test-design rules

- Red-green for behavior changes (Tasks 2, 3, 4): failing test first,
  confirm the failure mode matches the finding, then fix.
- Characterization tests (Task 1) are green-first: pin current behavior;
  teeth-check by temporarily inverting the key assertion (run once, observe
  failure, restore).
- Real processes and real files for lock/fork tests (`multiprocessing`
  spawn context or `subprocess`); no mocking of `os.open`, `fcntl`, or
  connections. Deadline-bounded polling, never bare sleep-then-assert.
- Multiprocessing child targets must be TOP-LEVEL module functions
  (picklable under spawn) — never nested/closure functions. Windows: the
  fork-based tests (Task 4) skip on Windows per the existing
  `test_fork_safety.py` style; Task 3.2's flock tests must ALSO carry a
  skip-on-Windows marker (fcntl semantics; the file runs on Windows CI
  otherwise). Task 2.4 runs cross-platform (spawn + flock via
  `AdvisoryFileLock`'s msvcrt path) — if it proves flaky on Windows,
  skip-marking it there with a comment is acceptable, recorded in
  Completion Notes.
- Watcher tests synchronize hard around lifecycle: `stop(join=True)` (or
  the suite's equivalent) before acting on the queue, and assert on
  collected-message lists only after join — a racy stop can make the skip
  test pass for the wrong reason.

---

## Task 0: Verify the watch-after plan's actual status

Verify BOTH halves of
`docs/plans/2026-07-02-watch-after-and-pg-rename-lock-plan.md` against
HEAD:

- Watcher half: read `watcher.py` `_consume_one_message`/`_consume_all_messages`
  and run the acceptance scenario manually (consume-mode watcher with
  `after_timestamp=X` must NOT deliver the boundary row `ts == X`).
- PG half: check `extensions/simplebroker_pg` — `prepare_queue_operation`
  with `operation="rename"` must take `LOCK_LAST_TS_ROW` before the rename
  scope lock, and the PG rename tests must exist. (Preliminary reading says
  both halves ARE landed.)

That plan file has a single top-level `Status:` line. Update it exactly as
follows: if BOTH halves hold, change `Status: proposed` to
`Status: implemented` and add directly beneath it one dated line:
`Verified implemented at HEAD (<short-sha>) during the 2026-07-03
checkpoint/lock plan's Task 0; watcher and PG rename halves both
confirmed.` If only one half holds, keep `Status: proposed` and add a dated
`Partial:` line naming which half is landed. Do not edit that plan's body
otherwise. Record findings in this plan's Completion Notes. 15 minutes; do
not skip — Task 1's wording depends on the watcher half.

Commit (only if the plan file changed): `Record watch-after plan implementation status`

## Task 1: Pin and document the move/checkpoint skip

**1.1 Characterization tests.** New file
`tests/test_move_checkpoint_semantics.py` with
`pytestmark = [pytest.mark.shared]` (public-API tests; see Conventions).
Backend binding: shared tests must reach the ACTIVE backend — construct
queues/watchers via the repo's shared-test fixtures (`workdir` with
implicit target resolution, or `broker_target`/`queue_factory`; see
`tests/conftest.py:~587,637,663`) — never `Queue(..., db_path=str(path))`
or `BrokerDB(str(path))` directly, which silently test SQLite even under
`bin/pytest-pg`/`bin/pytest-redis`. Follow an existing shared watcher test
for the pattern.

- `test_peek_watcher_skips_message_moved_in_behind_checkpoint` — run a
  peek-mode `QueueWatcher` on `dst` and deliver one fresh message so the
  checkpoint advances past ts T1; `stop(join=True)`; `move` a message with
  ts < T1 from `src` into `dst`; start a new watcher with
  `after_timestamp=<advanced checkpoint>` (durable-checkpoint consumer);
  assert within a deadline-bounded window that the moved message is NOT
  delivered while a freshly written message IS. (Do not re-test bare ts
  preservation — `tests/test_move_by_id.py::test_move_by_id_preserves_timestamp`
  already pins it.)
- `test_consume_watcher_without_filter_delivers_moved_message` — same
  setup, consume-mode watcher with NO `after_timestamp`: moved message IS
  delivered. This is the documented safe pattern.
- `test_consume_watcher_with_after_skips_moved_message` — consume-mode
  watcher WITH `after_timestamp=<checkpoint>`: moved message is NOT
  delivered (post-Task-0 semantics). Pins that consume+filter is also
  exposed.
- `test_cli_peek_after_skips_moved_message` and
  `test_cli_read_after_skips_moved_message` — the README caveat names
  `peek --after` and `read --after` explicitly, so pin them directly: via
  `run_cli` (which also confirms the module's `shared` marking is
  consistent with conftest's auto-marking), move a message behind a
  checkpoint ts and assert `peek --after <ts>` / `read --after <ts>` do
  not return it while a fresh message is returned.
- Teeth check per test-design rules.

**1.2 README caveat.** Two exact insertion points: (a) inside the
"Checkpoint-based Processing" section, immediately after its introductory
paragraph/example; (b) immediately after the "**Common options for
read/peek/move:**" block (README:~266) — there is no standalone `move`
section, and the "Move Mode (`--move`)" section (~:656) documents
`watch --move` and must NOT be used. Add a warning block with the same
visual weight as the consume-mode warning in "Critical Safety Notes":

> **Moved messages and checkpoints.** `move` preserves the message's
> original timestamp (stable IDs). Any timestamp-checkpoint consumer —
> `peek --after`, `read --after`, a peek-mode watcher, a consume-mode
> watcher started with `after_timestamp`, or any hand-rolled
> `ts > last_seen` filter — will **permanently skip** messages moved into
> its queue behind its checkpoint. If a queue receives `move` traffic,
> consume it without a timestamp filter, or periodically rescan from
> `--after 0`.

**1.3 Docstring caveats.** In `watcher.py`: at the peek-checkpoint advance
site and in the `QueueWatcher` class docstring near the existing consume
warning — two sentences stating the caveat, pointing at the README section.

**1.4 Consumer audit (OPTIONAL, read-only, cross-repo).** If
`/Users/van/Developer/weft` and `/Users/van/Developer/taut` are present:
grep for peek-mode watchers, `after_timestamp`, `--after`, and
`since_timestamp` consumers whose source queues can receive moves (weft:
reserved→inbox requeue is a `move`; check `weft/commands/tasks.py`
`since_timestamp=int(tid)-1` and any `QueueWatcher` usage; taut: its
watcher subclasses). Record each site and verdict (exposed / not exposed,
why) in Completion Notes. Do not change weft/taut code.

**1.5 CHANGELOG** (Unreleased → Documented).

Commit: `Document and pin move-vs-checkpoint skip semantics`

## Task 2: Fix phase-lock sidecar path collisions (and the dead CI gate)

**2.1 Fix the CI gate first** (it validates the rest of this task):
`.github/workflows/test.yml:58` — rename `_ENABLE_PHASELOCK_XATTRS` to
`PHASELOCK_ENABLE_XATTRS` (the name the code reads, `_phaselock.py:66`).
Run the fallback-gate's test list locally with the corrected env var BEFORE
making other changes and record the baseline (if the fallback path was never
exercised in CI, it may have latent failures — fix or report anything that
fails at baseline BEFORE proceeding; if a baseline failure is out of this
plan's scope, STOP and report).

**2.2 Red.** New tests in `tests/test_phaselock.py`:

- `test_same_stem_targets_get_distinct_sidecar_paths` — services for
  `tmp/mydb.db` and `tmp/mydb.backup` must have distinct `lock_path` AND
  distinct `status_base_path`. FAILS today.
- `test_xattrless_status_not_inherited_across_same_stem_targets` — with
  `PHASELOCK_ENABLE_XATTRS=0`, complete phases for `mydb.db`, then a
  service for `mydb.backup` must report NO completed phases. FAILS today.

**2.3 Green.** `_phaselock.py:506-507`: replace `with_suffix` with
append-construction (`Path(str(self.target) + lock_suffix)`, same for
status). Sweep `grep -n with_suffix simplebroker/_phaselock.py` for any
other sidecar derivations. Update the module docstring if it documents the
old naming. Then sweep the literal-path assertions:
`grep -rn "\.lock\b\|\.status\b" tests/test_phaselock.py
tests/test_runner_validation.py tests/test_runner_error_handling.py
tests/test_sqlite_setup_contention.py tests/test_queue_config_defaults.py`
and update old-style literals — these updates are the deliberate behavior
change, itemized in the CHANGELOG.

**2.4 Mixed-version safety test.** New test: run two concurrent setup
passes against ONE database file where one holds the old-style lock path
and one the new. Exact mechanism (`PhaseLockService`
derives paths from `target`/`lock_suffix`/`status_suffix` only,
`_phaselock.py:492` — it does NOT accept exact paths, and the production
services are constructed inside `SQLiteRunner._phase_lock_service()`):
spawn two child processes with a consistent spawn context — `ctx =
multiprocessing.get_context("spawn")`, then `ctx.Process` AND
`ctx.Barrier(2)` (a default-context Barrier shared into spawn children is
invalid on Linux). Child A
creates a real `Queue` against the shared db path — full runner setup, new
paths. Child B, INSIDE the child before creating its `Queue`, monkeypatches
`simplebroker._runner.SQLiteRunner._phase_lock_service` with a wrapper that
calls the original and then overrides the returned service's `lock_path`
and `status_base_path` to the old `with_suffix`-style values (comment:
simulates a pre-fix process; do not add a constructor parameter). Force real overlap and real work: pass both children a
`multiprocessing.Barrier(2)` and have each wait on it immediately BEFORE
its FIRST queue operation (the write) — NOT before `Queue(...)`
construction, which is lazy (setup runs when the first operation opens the
connection, `sbqueue.py:~158,320`), so a construction-time barrier would
not synchronize setup at all — and run the test
with `PHASELOCK_ENABLE_XATTRS=0` so completion markers live in the
per-scheme status files — child B's old-style status path means neither
child can observe the other's markers, so BOTH must execute full setup
(assert afterward that both status files exist and record completed
phases; a child that skipped setup would leave its status file absent).
Both children then perform one write+read. Parent asserts both children
exited 0, the database reports `PRAGMA journal_mode` = WAL, and a fresh
Queue can read/write (intact schema). This proves the accepted
mixed-version window is idempotent-safe rather than assuming it.

**2.5 Sweep.** `uv run pytest tests/test_phaselock.py -q` normally AND with
`PHASELOCK_ENABLE_XATTRS=0`; plus the four runner/contention test files
above; ruff/mypy.

**2.6 CHANGELOG** (Unreleased → Fixed): path collision; the CI gate fix;
the one-time setup re-run and orphaned old sidecar files.

Commit: `Derive phase-lock sidecar paths from the full target name`

## Task 3: Replace the vacuum lockfile with AdvisoryFileLock

**3.1 Path refactor FIRST (separate commit; mechanism unchanged).** Add a
module-level helper in `_backends/sqlite/maintenance.py`:

```python
def vacuum_lock_path(db_path: str | Path) -> Path:
    """Advisory lock sidecar for vacuum; appended, never with_suffix."""
    return Path(str(db_path) + ".vacuum.lock")
```

Switch the EXISTING `O_CREAT|O_EXCL` mechanism to use this helper (pure
path rename; the stale-mtime mechanism stays for now). NOTE: `vacuum()`
already has a LOCAL variable named `vacuum_lock_path`
(maintenance.py:159) — rename the local to `lock_path` first or the helper
call will shadow-fail. Update any test literals that encode the old
`with_suffix(".vacuum.lock")` path (`grep -rn "vacuum.lock" tests/`) to
import the helper. Run the vacuum test subset green. This ordering exists so that 3.2's red test exercises
the MECHANISM, not the path: a red test written against the new path while
the old code checks the old path would pass before the fix for the wrong
reason.

Also enumerate the actual phaselock exception classes (`grep -n "class
PhaseLock" simplebroker/_phaselock.py`) — the skip branch in 3.3 must catch
the exact exceptions `AdvisoryFileLock` raises on contention/timeouts (at
minimum `PhaseLockTimeout`, :80; check for an unavailable/platform
variant).

Commit: `Route the vacuum lock path through a shared helper`

**3.2 Red (mechanism).** New file `tests/test_vacuum_lock.py` (unmarked →
sqlite-only), written after 3.1 is committed so old-mechanism/new-path is
HEAD:

- `test_leftover_lockfile_does_not_block_vacuum` — create
  `vacuum_lock_path(db)` as a FRESH file (mtime now, younger than
  `BROKER_VACUUM_LOCK_TIMEOUT`) with PID content but NO flock held (SIGKILL
  leftover); write+read a message so a claimed row exists; run vacuum;
  assert the claimed row is gone. Under the 3.1 state this FAILS (the
  O_EXCL mechanism sees the file and silently skips); under flock it
  PASSES (no live flock holder → acquire succeeds).
- `test_concurrent_vacuum_skips_while_lock_held` — **NOT a red test;
  a semantics-preservation test, expected GREEN both before and after 3.3**
  (before: the flock's `a+b` open creates the lock file, so the O_EXCL
  mechanism sees it and skips; after: the flock itself blocks acquisition
  and vacuum skips — same observable behavior, different mechanism; that
  equivalence is exactly what this test pins). Hold the flock
  (`AdvisoryFileLock` on `vacuum_lock_path(db)`) from a real child process
  (acquire, signal readiness over a pipe, hold until told); run vacuum in
  the parent; assert it returns without error and the claimed row is still
  present; then release the flock, **delete the lock file (test-side
  reset — required for the pre-3.3 mechanism, whose O_EXCL check would
  otherwise still see the flock-created file; harmless post-3.3, where the
  mechanism recreates and flocks it)**, and assert a second vacuum
  succeeds. With that reset the test is green under both mechanisms. Only
  the leftover-lockfile test above is the red test for this task.

**3.3 Green.** In `vacuum()`:

- Use `vacuum_lock_path()`; delete the mtime-staleness pre-check,
  `_warn_stale_vacuum_lock`, the PID write, and the `O_CREAT|O_EXCL` open.
- Acquire `AdvisoryFileLock` non-blocking (timeout≈0); catch
  `PhaseLockTimeout` (only) and return silently; let any other exception
  propagate. Intent, decided here: `PhaseLockTimeout` covers both held-lock
  contention and lock-file open failures in `_phaselock.py` — treating both
  as "skip this opportunistic maintenance pass" is accepted (it slightly
  widens silent-skip relative to the old code, where non-contention
  `OSError`s from `os.open` propagated; vacuum is retried on later calls,
  so nothing is lost by skipping quietly). Do not add plumbing to
  distinguish the two cases. Never unlink the lock file.
- Rewrite the OLD-behavior tests deliberately:
  `tests/test_edge_cases.py::test_vacuum_lock_cleanup_after_crash` (crash
  recovery is now "kernel released the flock" — repurpose it to assert a
  post-crash vacuum proceeds immediately) and
  `::test_vacuum_lock_timeout_environment_variable` (the env var is inert —
  repurpose to assert it is still *parsed* without error and no longer
  gates anything). Sweep `grep -rn "vacuum.lock\|vacuum_lock" tests/` for
  other old-path/old-behavior literals.
- Update `BROKER_VACUUM_LOCK_TIMEOUT`'s description in BOTH places it is
  documented: `_constants.py` (~:595) and the README environment-variable
  table (~:1288, currently "Seconds before a vacuum lock is considered
  stale") — state it is deprecated and inert (kernel-released flock made
  staleness detection unnecessary).

**3.4 Sweep.** `uv run pytest tests/ -q -k "vacuum"` + the new file;
ruff/mypy.

**3.5 CHANGELOG** (Unreleased → Fixed + Deprecated).

Commit: `Serialize SQLite vacuum with a kernel-released advisory lock`

## Task 4: Complete fork guards; abandon, never close, inherited connections

**4.1 Red.** Extend `tests/test_fork_safety.py` (real `os.fork`;
skip-on-Windows per the existing style; child results over a pipe/exit
code):

- Parametrized guard tests: forked child calling each of
  `generate_timestamp` / `get_cached_last_timestamp` /
  `refresh_last_timestamp` / `sidecar` (enter the context) /
  `get_data_version` on an inherited core gets `RuntimeError` with the
  existing fork-safety message. Include one case through the public Queue
  path — and it must actually inherit the core: create the `Queue` with a
  persistent connection in the PARENT and exercise it once pre-fork (so
  the connection/core is bound), then fork and call
  `queue.generate_timestamp()` on the inherited object in the child (a
  default per-call Queue would build a fresh child-side connection and
  never hit the guard). FAILS today.
- `test_fork_fallback_abandons_without_close` — parent creates a runner +
  live connection; child triggers the pid-check fallback by invoking an
  operation on the INHERITED runner/core (e.g. `get_connection()` on the
  inherited `SQLiteRunner`, whose stored `_pid` differs from the child's
  `os.getpid()`) — a freshly constructed runner in the child would NOT
  exercise the fallback; child asserts over the pipe: (a) it received a NEW working
  connection, (b) the inherited connection object is present in
  `_ABANDONED_FORK_CONNECTIONS`, (c) reading `conn.total_changes` on the
  abandoned object does NOT raise `sqlite3.ProgrammingError` (i.e., it was
  not closed). No close-spy; no parent-side "still works" claims as
  evidence. To pin the abandon-ALL requirement (not just the current
  thread-local): before forking, create a SECOND tracked connection from
  another thread on the same runner (so `_all_connections` holds two
  inherited connections); the child asserts BOTH objects are present in
  `_ABANDONED_FORK_CONNECTIONS` exactly once each (identity-deduped) and
  neither raises on `total_changes`.

**4.2 Green.**

- `db.py`: `self._check_fork_safety()` as the first statement of the five
  methods, matching the 22 existing call sites' style.
- `_runner.py` fallback (~:211-219): replace the close block with appends
  to module-level `_ABANDONED_FORK_CONNECTIONS: list[Any] = []` (add the
  `Any` import if absent). Abandon the current `_thread_local.conn` AND
  every entry in `_all_connections`, **deduplicated by identity** (the
  current thread's connection is usually also tracked in
  `_all_connections` — do not append it twice; an `id()`-set or `any(c is x
  ...)` check suffices). Comment:

```python
# Do NOT close inherited connections: sqlite3 close() implies
# rollback and may touch the shared WAL-index the parent is still
# using (SQLite: a connection must not be used across a fork,
# including close). Abandon them: hold references forever so GC never
# finalizes them in this child. Bounded: one entry per inherited
# connection.
```

**4.3 Sweep.** Fork suite + `tests/test_thread_safety.py` +
`tests/test_sidecar.py` (update any test that forked-then-used a newly
guarded method expecting success — it was depending on the hazard); the
whole fast suite; ruff/mypy.

**4.4 CHANGELOG** (Unreleased → Fixed).

Commit: `Guard remaining fork entry points and abandon inherited connections`

## Task 5: Complete `commands.__all__`, bless the module; export `DatabaseError` from ext

**5.1 Red.** Extend `tests/test_ext_imports.py` (the existing ext-surface
contract test — NOTE it compares `ext.__all__` as a SET, not an ordered
list, `test_ext_imports.py:~95`; keep set semantics there): add
`DatabaseError` to the expected set (FAILS today). The commands-surface
test below, by contrast, asserts LIST equality (order included) against
the enumerated list. Add a commands-surface test (same file or
`tests/test_public_surface.py` if test_ext_imports is ext-only — check its
scope first): `simplebroker.commands.__all__` equals EXACTLY this ordered
20-name list (existing order preserved; the six new names inserted after
`cmd_status`, keeping `parse_exact_message_id` last):

```python
[
    "cmd_write", "cmd_read", "cmd_peek", "cmd_exists", "cmd_stats",
    "cmd_list", "cmd_delete", "cmd_move", "cmd_broadcast", "cmd_vacuum",
    "cmd_watch", "cmd_init", "cmd_status",
    "cmd_alias_list", "cmd_alias_add", "cmd_alias_remove",
    "cmd_rename", "cmd_dump", "cmd_load",
    "parse_exact_message_id",
]
```

FAILS today (6 `cmd_*` names missing).

**5.2 Green.**

- `commands.py`: replace `__all__` (:1237-1252) with the complete list —
  add `cmd_alias_list`, `cmd_alias_add`, `cmd_alias_remove`, `cmd_rename`,
  `cmd_dump`, `cmd_load`; keep `parse_exact_message_id`. Extend the module
  docstring: public embedding surface for CLI-equivalent operations
  (exit-code-returning, print-to-stdout semantics), stable under the same
  policy as package exports; reference the README section.
- `ext.py`: add `DatabaseError` to imports and the exceptions group of
  `__all__`. Then run the inventory (NOT export creep — see Locked
  Decisions) with an AST walk, not a grep (multi-line imports defeat
  line-grep):

```bash
uv run python - <<'EOF'
import ast, pathlib
for p in sorted(pathlib.Path("extensions").rglob("*.py")):
    if "test" in p.parts or "tests" in p.parts:
        continue
    for node in ast.walk(ast.parse(p.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module and (
            node.module.startswith("simplebroker._")
        ):
            names = ", ".join(a.name for a in node.names)
            print(f"{p}:{node.lineno}: from {node.module} import {names}")
EOF
```

  List the full output in Completion Notes; promote nothing else unless it
  is an exception class both extensions import (expected: none beyond
  `DatabaseError`; `OperationalError` etc. are already exported).
- README: "Command layer" subsection in Embedding — `simplebroker.commands`
  as the supported programmatic CLI equivalent (three example functions,
  exit-code contract); note `DatabaseError` is importable from
  `simplebroker.ext`. Note the narrow override of the earlier
  "documentation only" decision and why (exception-handling surface).

**5.3 Sweep.** Fast suite; `uv run mypy simplebroker
extensions/simplebroker_pg extensions/simplebroker_redis`; `bin/pytest-pg` /
`bin/pytest-redis` if Docker available (ext.py changed), else record the
skip.

**5.4 CHANGELOG** (Unreleased → Added).

Commit: `Complete commands exports and expose DatabaseError via ext`

## Task 6: Close-out

- Full suite including slow: `uv run pytest -q -m ""` (slow tests never run
  in CI; run them here and record results — CI policy itself is out of
  scope).
- Lint/type with the SAME ARGUMENTS as CI (this plan adds/edits tests, so
  a narrower local scope can go green while CI fails). CI invokes bare
  `ruff`/`mypy` inside its own environment (test.yml:95,99,103); locally,
  run the same argument lists through `uv run`. Re-check the workflow's
  argument lists at implementation time; as of this writing:

```bash
uv run ruff check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests --config-file pyproject.toml
```
- Fill in Completion Notes: Task 0 verification, Task 1.4 audit (or its
  skip), Task 5.2 inventory, baseline results from Task 2.1, deviations.
- Flip this plan's Status to `implemented`, fill Completion Notes, and
  commit the plan-file update itself.
- Verify each task's commit exists with the exact message via `git log`.

Commit: `Mark checkpoint-move and lock hardening plan implemented`

## Completion Notes

Implemented 2026-07-03 against baseline HEAD `5a59d52`. `docs/` is gitignored in
this repo, so plan-file status flips (this file and the watch-after plan) are
file edits only, NOT committed (verified: `git add` refuses the ignored path).

Commit hashes (one per task; Task 3 has two):
- Task 0: (no commit — docs-only status flip of the watch-after plan)
- Task 1: `058f51e` Document and pin move-vs-checkpoint skip semantics
- Task 2: `ac1f33f` Derive phase-lock sidecar paths from the full target name
- Task 3.1: `63ee73b` Route the vacuum lock path through a shared helper
- Task 3.3: `793269d` Serialize SQLite vacuum with a kernel-released advisory lock
- Task 4: `25cfb53` Guard remaining fork entry points and abandon inherited connections
- Task 5: `845b6b0` Complete commands exports and expose DatabaseError via ext
- Close-out: `37f00dd` Reformat fork-fallback abandon block to ruff line width
  (DEVIATION — see below)
- Task 6: (no commit — docs-only status flip of THIS plan)

### Task 0 verification
Both halves of `2026-07-02-watch-after-and-pg-rename-lock-plan.md` confirmed
landed at HEAD `5a59d52`: watcher half (`_consume_one_message` :1562 /
`_consume_all_messages` :1587 both apply `after_timestamp` via
`_after_timestamp_filter` :1550; consume-mode acceptance tests pass with the
boundary row excluded), PG half (`prepare_queue_operation(operation="rename")`
plugin.py:708 runs `LOCK_LAST_TS_ROW` then `LOCK_RENAME_SCOPE`; lock-order tests
exist). That plan flipped to `Status: implemented` (file edit only).

Implementation note: the watcher half uses `read_many(1, after_timestamp=...)`
for the single-message consume path rather than the `stream_messages` shape the
older plan sketched; the strict filter still reaches the claim query, so the
invariant holds.

### Task 1.4 consumer audit (weft + taut both present, read-only)
No weft consumer is exposed to the move-skip. weft's reserved→inbox requeue
(`reserved.move_one/move_many`, consumer.py/pipeline.py/manager.py) lands old-ts
rows in the inbox, but the inbox is consumed by `MultiQueueWatcher`
(`_fetch_next_message`, multiqueue_watcher.py:603) via `read_one` with NO
`after_timestamp` — the documented SAFE pattern. `queue_wait.py:47`'s peek
watcher with `after_timestamp=queue.last_ts` is only a readiness signal
(`_handle_queue_activity` sets an Event). All `since_timestamp=int(tid)-1` and
`after_timestamp` reads target `weft.log.tasks` (append-only log; never receives
moves). taut: no simplebroker `after_timestamp`/peek-watcher consumers tied to
move-target queues. No weft/taut code changed.

### Task 2.1 baseline
The corrected CI fallback-gate env var (`PHASELOCK_ENABLE_XATTRS=0`) run locally
over the 5-file gate list: ALL PASS, only environment-specific skips (Darwin
libc xattrs unsupported on this filesystem; msvcrt Windows-only). No latent
failures in the previously-unexercised fallback path — proceeded.

### Task 5.2 AST inventory
Extension private imports from `simplebroker._*` enumerated via AST walk.
`DatabaseError` imported by BOTH extensions (pg: plugin.py:15, validation.py:14;
redis: plugin.py:16, pool.py:11, validation.py:14). `OperationalError` imported
by both but already in `ext.__all__`. No exception class beyond `DatabaseError`
needs promotion — matches the plan's expected "no new export beyond
DatabaseError". (Full listing in the implementation report.)

### Test results
- Full suite including slow (`uv run pytest -m "" -ra`): 1406 passed, 13 skipped,
  0 failed.
- CI-equivalent lint/type (via `uv run`, exact CI argument lists from
  test.yml:95/99/103): `ruff check` clean, `ruff format --check` clean (after
  the close-out reformat), `mypy` clean (66 source files).
- Container suites (Docker available): `bin/pytest-pg` (ext surface + full pg
  extension suite) and `bin/pytest-redis` (full redis extension suite) all pass.
  Task 1's new characterization tests also verified GREEN under the real
  Postgres backend (active-backend binding confirmed).

### Deviations
1. **Close-out reformat commit `37f00dd`.** Task 4's committed `_runner.py`
   abandon block contained one line exceeding ruff's line width. The per-task
   local ruff check in Task 4 targeted specific test files and missed it; the
   Task 6 CI-equivalent `ruff format --check` (which scans all of
   `simplebroker`) caught it. Because Task 5 was already committed on top of
   Task 4, amending Task 4 would have required an interactive rebase; instead
   the pure line-wrap reformat (behavior-verified unchanged by re-running the
   fork suite) was committed separately as close-out hygiene. This is the only
   extra commit beyond the plan's prescribed per-task commits.
2. **Task 0 / Task 6 status-flip commits skipped** because `docs/` is gitignored
   (documented in the plan's execution notes as the expected path). Both status
   flips are file edits on disk only.

### Post-review P1 fix (dual-review follow-up)
Codex review found a P1 in the Task 4 area: the `current_pid != self._pid`
fork-recovery branch in `_runner.py` acquired INHERITED locks
(`_connections_lock` at the abandonment snapshot and again when clearing;
`_setup_lock` when resetting phases — those last two acquisitions predate this
plan). If any parent thread held one of those locks at `fork()` time, the child
(single-threaded under POSIX fork) could never release it and
`get_connection()` hung before abandoning connections. Fixed in commit
`Replace inherited runner locks during post-fork recovery`: the recovery branch
now snapshots `_all_connections` WITHOUT the lock (safe — the child is
single-threaded; nothing in-process mutates the set), abandons the refs
(identity-deduped as before), then replaces `_setup_lock`,
`_connections_lock`, and `_operation_lock` with fresh objects before any
child-side acquisition, and clears child-local state under the fresh locks.
Audit of the rest of the recovery chain
(`_recover_completed_phases_from_markers` → `is_setup_complete` →
`_has_valid_completion_marker`): only the (now fresh) `_setup_lock` is
acquired; `has_phase`/`discard_status_markers` are passive file operations with
no advisory-lock or module-level `_PROCESS_LOCKS` acquisition. Red-green:
`test_fork_recovery_does_not_block_on_inherited_locks` (parent thread holds
`_connections_lock` across the fork point; watchdog-bounded `waitpid`) HUNG
pre-fix (watchdog killed the child; test failed with the hang diagnostic) and
passes post-fix. Fork suite + thread-safety suite + CI-argument ruff/mypy all
green after the fix.

**Follow-up (Codex re-verification):** the first fix closed only the
`get_connection()` path — `run`/`begin_immediate`/`commit`/`rollback`/`close`
acquire `_operation_lock` BEFORE calling `get_connection()`, so a forked child
still hung on the inherited RLock before reaching the recovery branch. Fixed
structurally in commit `Run fork recovery before acquiring any runner lock`:
the recovery block is extracted into `_recover_after_fork_if_needed()`
(idempotent; first-line pid check is a cheap no-op in the normal case; `_pid`
is updated before marker recovery so the fork-guarded `is_setup_complete`
re-entry is a no-op) and called at the TOP of every entry point that acquires a
runner-level lock: `run`, `begin_immediate`, `commit`, `rollback`, `close`,
`get_connection`, `run_exclusive_setup`, `is_setup_complete`. Post-audit: every
`_operation_lock`/`_setup_lock`/`_connections_lock` acquisition in `_runner.py`
now sits behind a guarded entry point (or inside recovery itself, on fresh
locks). A child's `close()` now abandons inherited connections instead of
closing them. Red-green sibling test
`test_fork_recovery_runs_before_operation_lock` (parent thread holds
`_operation_lock` across fork; child calls `run("SELECT 1", fetch=True)`) HUNG
pre-fix (watchdog) and passes post-fix. P2 also fixed: the fork tests added by
this plan now assert `os.WIFEXITED(status)` before `os.WEXITSTATUS(status)` so
a signaled child cannot read as exit 0 (pre-existing fork tests at
test_fork_safety.py:50/:92 retain the old pattern — outside this plan's
changes).

## Explicitly Out of Scope

- Any change to move timestamp semantics (re-stamp option) — separate
  product decision.
- The `watch --after` consume-mode fix — owned by the 2026-07-02 plan
  (Task 0 only records its status).
- CI policy changes (slow-test inclusion, fuzz cadence) beyond the one
  broken env var in Task 2.1.
- Publishing `docs/` — separate decision.
- weft/taut code changes arising from Task 1.4 — file in those repos.
- A lease primitive — the evaluation's design probe recommends
  weft-resident; nothing to do here.
- Promoting any extension private import beyond `DatabaseError` — the
  lockstep-private contract (`ext.py:7-27`) stays as-is.

## Review Log

- Round 1 (Codex, 2026-07-03): 30 findings — stale anchors, false premises
  (commands `__all__` existed but incomplete; watch-after plan already
  landed), missing existing-test updates, fork-test design flaws,
  convention conflicts. All incorporated.
- Round 2 (Codex, 2026-07-03): 4 findings — abandon-all inherited
  connections (not just current thread-local), Task 2.4 mechanism
  under-specified, `OperationalError` already exported, close-out commit
  missing. All incorporated.
- Round 3 (Codex, 2026-07-03): 4 findings — Task 3 red test crossed old/new
  paths (restructured into 3.1 path refactor + 3.2 mechanism red), Task 0
  status-edit ambiguity, Task 2.4 exact monkeypatch route, AST-based
  import inventory. All incorporated.
- Round 4 (Codex, 2026-07-03): 10 findings — concurrent-vacuum test
  reclassified as green-both-sides semantics test, local-variable
  shadowing in `vacuum()`, README env-var doc, inherited-runner fallback
  trigger, identity-dedupe on abandonment, Task 2.4 overlap forcing +
  must-run-setup assertions, exact ordered `__all__`, exact README move
  anchors, stale header metadata, plan path. All incorporated.
- Round 5 (Codex, 2026-07-03): 5 findings — concurrent-vacuum second
  assertion needed a test-side lock-file reset to be green both sides,
  Queue guard test must use a persistent pre-fork connection, abandonment
  test must pin ALL tracked inherited connections, `peek/read --after` CLI
  characterization tests added, CI-equivalent lint scope. All
  incorporated.
- Round 6 (Codex, 2026-07-03): 3 findings — Task 2.4 barrier moved to the
  first queue operation (Queue setup is lazy), exact CI mypy command
  (bin/release.py + per-package extension paths), ext test uses set
  semantics vs commands test list equality. All incorporated.
- Round 7 (Codex, 2026-07-03): 2 findings — close-out ruff commands now
  verbatim from test.yml:93-101; README caveat anchor (b) pinned to the
  "Common options for read/peek/move" block (no standalone move section
  exists). All incorporated.
- Round 8 (Codex, 2026-07-03): 3 findings — spawn-context Barrier
  (`ctx.Barrier`/`ctx.Process`), "verbatim" reworded to same-arguments-as-CI
  via `uv run`, PhaseLockTimeout catch scope decided (skip on both
  contention and open failure, other exceptions propagate). All
  incorporated.
- Round 9 (Codex, 2026-07-03): 2 findings — shared tests must bind the
  active backend via the shared-test fixtures (no direct db_path
  construction); multiprocessing child targets top-level/picklable and
  Windows skip policy per test family. All incorporated. No factual errors
  remained.
- Round 10 (Codex, 2026-07-03): No remaining findings. Verdict: "I could
  confidently and correctly implement this plan exactly as written."
  Plan approved for implementation.
