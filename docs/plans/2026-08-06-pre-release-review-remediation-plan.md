# Pre-Release Review Remediation Plan (6.0.2 / pg 3.5.1 / redis 3.5.1)

Date: 2026-08-06
Revision: 6 (2026-08-07) — owner drops Unit B's proposed forced-mode
hardening after deployment/threat-model review. SQLite filesystem permissions
remain operator-managed and the release documents the full multi-user access
condition instead of promising one exact mode.
SQLite `--cleanup` is deliberately destructive: it deletes all known
SimpleBroker-owned filesystem state for the configured target without
quiescence, and the storage outcome under overlapping SQLite clients is
explicitly undefined per SQLite upstream. The CLI's deletion-attempt,
diagnostic, and exit semantics remain defined. The round-2 owner amendments (no fractional bounds; Weft
unaffected; asynchronous release mechanism retained) continue to outrank
conflicting older text.
Status: active — initial implementation landed in `a38e6a9`; CI remediation
and release-gate passes are recorded below. The final blocker (conflicting durable
lesson) was corrected in place, and the
owner's implementation direction authorized A, E, I, and J (B is reduced to
documentation in J; C/D/F/G/H are deferred and severed). The plan remains
active: no release tag, publication, or K0/K release execution occurred in
this pass. Unit E's revision-5 destructive contract
passed the recorded focused primary, interface, and independent lifecycle
re-reviews below.
Class: 5 — [DOM-6] fires: normative deltas to `docs/specs/10-cli.md`
[SB-CLI-5] (bound-string grammar), `docs/specs/17-ops.md` [SB-OPS-7]
(destructive target cleanup), and `docs/specs/16-python-library-api.md`
[SB-API-11] (public timestamp-validator grammar). [DOM-5] risky triggers
also fire (public contract narrowing; destructive persistence behavior;
persistence-security behavior; release one-way door), so the hardening-plans
checklist applies.
Promotion strategy: **B — atomic** per delta (text, code, tests, and
CHANGELOG land together in each owning unit).
Theory: grounded — [THEORY-3] (backend adapter/runner owns
substrate-resource lifecycle) governs Unit E's cleanup-ownership
disposition (F5); [REV-THEORY-005] (suspended operations retain their
ownership context) governs Unit G's deferral to a dedicated lifecycle
redesign (F7). Revision 1's `Theory: N/A` waiver was reviewed as false
for the watcher unit and is withdrawn.

## Goal

Close the pre-push findings from the 2026-08-06 five-domain
re-evaluation that are safely closable **within this release train**,
so the staged 6.0.2 / simplebroker-pg 3.5.1 / simplebroker-redis
3.5.1 release ships without the known contract untruths — then execute the
batch release from one green SHA through the existing asynchronous release
machinery. The reduced train: the timestamp-grammar contract fix (A), a
truthful SQLite filesystem-permission limitation in the docs (B/J), explicitly
destructive SQLite target cleanup (E), CI probe wiring across
all three owning suites (I), the docs truthfulness batch (J), and release
mechanics + execution (K0/K). Unit E does not add a quiescence mechanism. It
defines the exact deletion namespace and makes SQLite's upstream undefined
concurrent-storage boundary part of the public contract. Five findings that revision
1 treated as small are in fact architectural (bootstrap, CLI UX
policy, pg lock ordering, watcher lifecycle, redis state machines);
they are deferred with named reopen conditions, not silently dropped.

## Source Documents

- Theory: `docs/program-theory.md` [THEORY-3] (backend adapter/runner
  ownership row — grounds the decision that any future cleanup redesign belongs
  to the backend lifecycle owner) and
  [REV-THEORY-005] (grounds Unit G's deferral: watcher cleanup is an
  ownership/lifecycle question requiring its own redesign, not a
  finalizer patch).
- Governing spec clauses:
  - `docs/specs/10-cli.md` [SB-CLI-5] (non-exact bound string forms —
    normative delta 1)
  - `docs/specs/16-python-library-api.md` [SB-API-11] (public
    `TimestampGenerator.validate()` grammar — linked from normative delta 1)
  - `docs/specs/17-ops.md` [SB-OPS-7] (destructive target cleanup and
    SQLite concurrent-storage boundary — normative delta 2)
  - `docs/specs/13-message-identity.md` [SB-ID-4] (read-only context:
    exact-ID acceptance does NOT change; exact-insert immediacy is
    also why the deferred redis work must not blanket-retry)
  - `docs/specs/11-delivery.md` [SB-DELIVERY-6] (context: the
    finalization probes Unit I wires into CI)
  - `docs/specs/10-cli.md` [SB-CLI-1]/[SB-CLI-2] (exit-code and
    stream discipline all units must preserve; the reason Unit D is
    deferred rather than patched)
- Executable release authority: `bin/release.py` (batch target order
  pg → redis → core at :374–378; standalone-core refusal of unpublished
  extension baselines at :1899–1915 and :2465–2467; tag-push loop at
  :2065–2107; batch execution at :2214–2335) and its tests.
  Revision 2 adapts to this machinery instead of contradicting it.
- Evaluation evidence: the 2026-08-06 re-evaluation findings pinned in
  the maintainer's review-debt ledger; per-finding citations inline.
- Independent review: `## Independent Review Findings (2026-08-06)`
  below (F1–F10, BLOCKED) — the primary input to this revision.
- Runbooks: `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`.
- Permission-policy precedent (read-only context): POSIX `open()` applies the
  process file-creation mask to the requested mode
  (<https://pubs.opengroup.org/onlinepubs/9799919799/functions/open.html>);
  SQLite's ordinary Unix database default is `0644`
  (<https://sqlite.org/compile.html>); Git respects umask by default and exposes
  shared-repository permissions as an explicit operator choice
  (<https://git-scm.com/docs/git-config/2.44.3.html#Documentation/git-config.txt-coresharedRepository>);
  Windows files and directories use security descriptors and inherited ACLs
  from the containing directory by default
  (<https://learn.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights>).
- Upstream destructive-concurrency boundary: SQLite's
  [How To Corrupt An SQLite Database, unlink/rename section](https://www.sqlite.org/howtocorrupt.html#unlink)
  states that Unix can retain open old and replacement database generations
  under one pathname, that pathname-derived journal/WAL names can be shared,
  and that the resulting behavior is undefined and probably undesirable.

## Spec Baseline

- `1c01254` — spec tree and product code at plan authoring time.
  Review rounds 1–3 ran against working tree at `3cb6e09`. Plan type:
  implementation with spec revision (strategy B, atomic, two live proposed
  normative deltas; the [SB-API-11] timestamp-validator link travels with
  Delta 1; revision 3's quiescent-cleanup text is rejected historical text and
  is replaced by revision 5's destructive-cleanup contract).
- Promotion baseline: `3cb6e09` plus the uncommitted 2026-08-07 worktree
  promotion in `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`,
  `docs/specs/17-ops.md`, `docs/specs/product-section-registry.md`,
  `docs/specs/00-specs-index.md`, and `llms.txt`. Implementation is checked
  against those promoted canonical sections, not the appendix text below.

## Owner Amendments During Round 2

These decisions outrank and supersede conflicting text later in this plan.

1. **Fractional seconds are unsupported in every accepted bound spelling.**
   Bare numeric, unit-suffixed numeric, and ISO-8601 inputs must not contain a
   fractional-second component. Users needing finer granularity use integer
   milliseconds (`ms`), integer nanoseconds (`ns`), or a native hybrid message
   ID. This is a documented public limitation, not a compatibility accident.
2. **Weft does not depend on fractional bounds.** Its direct parser call remains
   a downstream smoke surface for retained integer/ISO forms, not a reason to
   preserve fractions.
3. **The current release-tag path is accepted.** It is working operationally and
   remains asynchronous. Round 2 therefore does not require serialization or a
   release-driver change. K0 must describe tag-push order separately from
   unspecified workflow/PyPI completion order and name rerunning a failed
   existing workflow as recovery; this is plan truthfulness, not a product
   defect or release blocker.

## Superseded Owner Amendment After Round 3 (Revision 4)

This historical decision was superseded by revision 5. It explains why the
revision-4 text below deferred Unit E, but it is not implementation authority:

1. **Full or concurrent SQLite cleanup is out of scope.** Unit E is deferred;
   this release does not add a sidecar marker, xattr, connection-lifetime lock,
   new backend result type, or new cleanup lifecycle contract.
2. **Document the current limitation directly.** SQLite `--cleanup` is an
   offline maintenance operation. Users must stop all clients first. It deletes
   the main database file only; same-basename `-wal`/`-shm` files may remain,
   and persistent phaselock files such as `.lock` are intentionally outside
   cleanup. There is no concurrent-cleanup or quiescence guarantee.
3. **Future reopening requires evidence and its own design.** A marker or xattr
   can record ownership but cannot prove quiescence. An enforceable guarantee
   would require lifetime coordination across every cooperative connection,
   would not cover external SQLite or older clients, and belongs in a separate
   class-5 lifecycle plan if user harm justifies that cost.

## Owner Amendment After Experimental Re-review (Revision 5)

This is the active Unit E decision and supersedes the revision-4 deferral and
every earlier quiescence, marker, xattr, or lifecycle-lock proposal:

1. **`--cleanup` is destructive, not protective maintenance.** For SQLite, an
   explicit cleanup authorizes deletion of all known SimpleBroker-owned state
   for the configured target: the main path; `<path>-journal`; `<path>-wal`;
   `<path>-shm`; `<path>.lock`; `<path>.status`; `<path>.vacuum.lock`; and
   crash-residue entries matching
   `<path>.status.tmp.<decimal-pid>.<decimal-time_ns>`. It authorizes no other
   directory scan or glob.
2. **Do not wait for quiescence.** Apart from the existing short-lived,
   read-only ownership validation when a main file exists, cleanup opens no
   SQLite connection. It takes no SQL transaction, checkpoints nothing, and
   does not refuse merely because another client may exist. Anything such a
   client has committed, buffered, or may later write is inside the destructive
   operation's blast radius.
3. **Concurrent storage outcomes are undefined.** SQLite upstream explicitly
   says unlinking or renaming an open database on Unix yields undefined and
   probably undesirable behavior, including the risk that old and replacement
   database generations share pathname-derived journal/WAL names. The spec
   therefore promises no exact database, durability, visibility, error, file
   recreation, or disk-reclamation outcome when any SQLite connection overlaps
   cleanup. Active SimpleBroker setup, phaselock, status-publication, or vacuum
   operations are in the same blast radius: deleting a held POSIX lock path can
   let another process create and lock a distinct inode. Concurrent replacement
   or mutation of the target directory entries is also undefined; validation
   is not a stable-inode lease. This is a limitation, not a safety guarantee.
4. **The CLI contract remains deterministic.** Exit status and diagnostics
   report validation and the command's ordered unlink attempts. A deletion
   failure is best-effort: later candidates are still attempted, then one
   stderr error reports the failed attempts and the command exits nonzero. It
   neither retries nor rolls back. Success means every owned namespace entry
   observed during its attempt was absent or unlinked. It does not mean the
   names remain absent after the attempt, and it says nothing about an
   overlapping client's result.
5. **Owned-namespace authority replaces orphan ownership proof.** If the main file
   exists, cleanup validates it as a SimpleBroker database before deleting any
   namespace entry. If the main file is absent, the explicit destructive flag
   still authorizes unlinking the complete owned namespace above. Symlinks at
   those names are unlinked as directory entries and never followed. This makes
   a partial cleanup retry useful while keeping the boundary bounded.

## Owner Amendment After Permission Deployment Review (Revision 6)

This is the active Unit B decision. It supersedes the proposed secure-create
helper, exact-`0600` guarantee, reopen normalization, public `SQLiteRunner`
mode delta, file-mode tests, and every earlier statement that Unit B is a
release-blocking security flaw:

1. **No demonstrated release-blocking threat.** The reported `0644` companion
   mismatch is POSIX-specific, but the deployment conclusion is cross-platform:
   file access is governed by both the file and its containing path. A file mode
   such as `0644` is not independently an exposure when an unintended user
   cannot traverse the containing path; on Windows, the equivalent boundary is
   the effective file/directory ACL. In a shared location, ownership, group or
   principal membership, ACLs, directory permissions, existing permissions,
   platform behavior, and (on POSIX) the process umask are deployment policy.
   Queue bodies can be sensitive, so operators still need an explicit access
   boundary; SimpleBroker does not assume that every broker is secret or
   override that policy with a universal owner-only permission set.
2. **Do not force or normalize one mode.** This release adds no pre-create
   helper, `chmod` normalization, exact-mode invariant, config key, or
   `SQLiteRunner` creation contract. In particular, it does not silently revoke
   group access on an existing database. Existing code behavior is unchanged.
3. **Document the complete cross-user condition.** For access by more than one
   OS user, the filesystem must grant every intended writer effective read and
   write access to the broker database and every associated file that exists or
   may be created:
   `<db>-journal`, `<db>-wal`, `<db>-shm`, `<db>.lock`, `<db>.status`,
   `<db>.status.tmp.<pid>.<time_ns>`, and `<db>.vacuum.lock`. The containing
   directory must let every intended writer traverse it and create, replace,
   and remove those entries. On POSIX this requires the appropriate directory
   write and execute/search bits or ACL; on Windows it requires the equivalent
   file and directory ACL rights. SimpleBroker does not promise to provision or
   preserve a group-sharing policy; the operator owns it.
4. **Document the private-directory consequence.** When the broker is inside a
   directory that excludes unintended users, a more permissive file permission
   alone does not make the broker accessible to them. For sensitive content,
   recommend a directory restricted to the intended users through POSIX
   ownership/modes/ACLs or Windows ACLs; on POSIX also use a suitably restrictive
   umask. Do not claim that every artifact has one exact mode or that umask alone
   can grant group write access.

## Context and Key Files

### A. Timestamp bound-string grammar (F1 — rewritten)

`simplebroker/_timestamp.py` — `TimestampGenerator.validate()` is the
public string-parser choke point. **Corrected surface map (F1 plus owner
amendment):** the
CLI flags are `--after`/`--before` (there is no `--since`); library
`after_timestamp=` accepts integers and does not route through the
string parser; Weft consumes the parser directly at
`../weft/weft/commands/queue.py:939–943` and `:1154`, but the owner has
confirmed that it does not depend on fractions. Suffix parsing
(`_timestamp.py:355,411,456`) runs **before** the point revision 1 proposed
to gate. Bare and suffixed fractions are also pinned by existing tests, so
the implementation must remove those accepted paths deliberately rather
than only adding the underscore/sign gate.

The contract is therefore specified as **three separate grammars**,
each with its own acceptance rule after whitespace stripping and
digit folding:

1. **ISO-8601** — `datetime.fromisoformat`-compatible grammar, restricted to
   integral seconds. `+`/`-`/`:` retain their ISO meanings; a decimal point or
   decimal comma in the seconds component is rejected.
2. **Unsuffixed numeric** — the entire candidate must satisfy
   `str.isdecimal()` before any digit-count unit classification or
   `int()` call. Rejects `1.532`, `1_705_329_000`, `+99999999999`, and
   every other non-integral or `int()`-tolerated non-decimal spelling.
3. **Suffixed numeric** (`<digits><unit>`, e.g. `1705329000s`,
   `1705329000500ms`, `1705329000500000000ns`) — the entire number
   portion before a recognized `s`, `ms`, or `ns` suffix must satisfy
   `str.isdecimal()`. Fractions such as `1705329000.5s` and `1.5ms`,
   underscores, and signs are rejected.

A candidate failing all three grammars gets an actionable bound-parse error
that identifies the integer-only limitation and points to integer `ms`,
integer `ns`, or a native hybrid ID. Defect being fixed (live-reproduced):
`"1705329000"` →
2024-01-15 while `"1_705_329_000"` (same value) → 1970-01-20, because
the length heuristic counts separators as digits. Exact message-ID
acceptance ([SB-ID-4]) is documented behavior and does not change.

### B. SQLite filesystem permissions (revision-6 scope reduction)

The review found a real first-session POSIX mode mismatch, but the label
“security flaw” was not backed by a deployment threat model. On POSIX, access
depends on the containing directory as well as each file; on Windows, effective
access depends on the file/directory ACLs and their inheritance. A forced
owner-only policy would override valid operator choices and still would not
define a coherent cross-user deployment unless it covered the entire SQLite
and SimpleBroker namespace.

This matches ordinary platform practice rather than secret-key practice. POSIX
file creation starts from a requested mode and removes bits through the process
file-creation mask; SQLite's standard Unix build requests `0644`. Git uses umask
by default and makes shared-repository permissions an explicit operator choice.
Windows ordinarily assigns new filesystem objects security descriptors whose
ACLs inherit from the parent directory. SimpleBroker broker data is general
application data, not an inherently secret credential such as a private key, so
the release does not invent a stronger universal default without a demonstrated
product threat.

Unit B therefore has no product-code implementation in this train. T-B is a
documentation correction owned by J. It removes the false uniform-`0600`
statement and records the exact multi-user condition and complete associated
file list from the revision-6 owner amendment. A future automatic permission
policy reopens only on demonstrated user harm or an explicit product decision
to support cross-OS-user SQLite deployments; that design must cover the main
database, all SQLite companions, every SimpleBroker coordination/maintenance
file, directory access, ownership/group/ACL/umask interaction, recreation, and
non-POSIX behavior together.

### E. Destructive SQLite `--cleanup` (revision-5 owner decision)

Owner: the SQLite backend plugin. Boundary: only the resolved configured target
and the closed owned-namespace allowlist below. Verification: real
CLI/filesystem tests plus the [SB-OPS-7] firing matrix below. Required action:
implement this state machine without a read-write/lifecycle SQLite connection
and without trying to preserve work from concurrent clients.

1. Derive the exact paths once: `main`, `main-journal`, `main-wal`, `main-shm`,
   `main.lock`, `main.status`, and `main.vacuum.lock`. Also enumerate only
   entries matching `main.status.tmp.<decimal-pid>.<decimal-time_ns>` in the
   same directory, because phaselock atomic status replacement can leave that
   crash residue. Both variable components must be nonempty ASCII decimal
   digits. Sort those dynamic entries by filename for deterministic diagnostics.
   Never match another spelling or scan recursively. Path derivation and main
   `lstat` form the safety preflight: an error other than a missing main aborts
   with zero deletions because ownership validation cannot proceed. Status-temp
   enumeration is best-effort: a missing parent means no temp entries; any
   other enumeration error is recorded, every fixed owned name is still
   attempted, and the error is reported with any unlink failures. Matching
   entries yielded before a mid-iteration error remain frozen, are sorted, and
   are attempted before the fixed names.
2. Treat `:memory:` and the empty SQLite target as successful no-ops and derive
   no filesystem names for them. For a filesystem target, freeze one expanded,
   resolved path for validation and every owned-name derivation.
3. Inspect the main entry. If it exists, require the existing SimpleBroker
   validation before any unlink. A foreign, invalid, unreadable, or directory
   main target is a zero-delete error. If main is absent, skip validation; the
   explicit destructive command authorizes the complete orphan namespace.
   Repair `validate_database()` to construct its read-only URI with the standard
   path-to-URI encoder; the current hand-built `file:{path}?mode=ro` can validate
   the wrong pathname when the filename contains `?`, `#`, or `%`.
4. Attempt status-temp residues, `.status`, `.vacuum.lock`, `.lock`, `-journal`,
   `-wal`, `-shm`, then `main`, using unlink semantics that do not follow a
   symlink at any owned name. Fixed candidates use direct unlink with no prior
   existence check: successful unlink counts as found, and `FileNotFoundError`
   counts as absent. Main and enumerated temp entries observed earlier still
   count as found if they disappear before their unlink attempt.
   Attempt every authorized path even after one unlink fails, then report every
   unlink attempt that failed. The order keeps the validated main name
   available until the final attempt and gives a non-concurrent retry the best
   chance of clearing a partial result.
5. Preserve the backend `cleanup_target(...) -> bool` compatibility contract:
   return `True` when the main existed at preflight, a status-temp entry was
   enumerated, or an unlink succeeded, and every attempt succeeded. An entry
   observed in preflight/enumeration that disappears before unlink still counts as `True`;
   a fixed sidecar never observed and absent at its direct unlink does not.
   Return `False` only when the whole owned namespace was unobserved/absent.
   If enumeration or any unlink fails, raise one `DatabaseError` after all
   possible attempts. It names the failed attempts in deterministic order and
   warns that other entries may already be gone. Enumeration failure appears
   first, followed by unlink failures in attempt order; it does not call them current
   residue, retry automatically, or roll anything back. The CLI maps
   success/no-op/errors through existing
   [SB-CLI-1]/[SB-CLI-2] streams and exit classes; `--quiet` suppresses success
   and no-op status only, never errors.
6. Do not perform a final absence check and do not promise post-return absence.
   Another process can recreate a name immediately. Under an overlapping
   SimpleBroker client, setup/phaselock/status/vacuum operation, or raw SQLite
   connection, all storage, coordination, and client-observed results are
   undefined by contract; SQLite's storage warning is also explicit upstream.
   The command's defined result is limited to its own validation and unlink
   attempts.

The destructive tradeoff is intentional. A stale POSIX client may error, or it
may continue committing successfully to unlinked old-generation inodes; a new
client may create a replacement generation at the same pathname. In the
revision-5 diagnostic probes, stale writers continued, their work vanished on
close, replacement writers remained healthy, and unlinked files retained disk
until close. Those are observed examples only. They are not promised outcomes:
SQLite warns that old and replacement generations may share pathname-derived
journal/WAL files and that the result is undefined. Windows ordinarily refuses
unlink of an open database or sidecar; those unlink failures use the same
defined aggregate-error path, with any prior deletions still irreversible.

### I. CI probe wiring (F9 — three owning workflows)

Corrected premise (F9 plus round-2 review): root pytest collects only
`tests/`; the pg and redis probes live in their extension suites with their
own workflows. One root-job env var can neither run the extension probes nor
avoid multiplying across the matrix. Fix: **one explicit non-coverage Linux
probe step in each owning workflow**: `.github/workflows/test.yml`,
`.github/workflows/test-pg-extension.yml`, and
`.github/workflows/test-redis-extension.yml`, conditioned to one declared
Python matrix member. Each step sets and asserts
`SIMPLEBROKER_RUN_FINALIZATION_PROBE=1` before invoking its probe file
directly. Exit 5 guards collection only; a missing opt-in variable skips every
test and exits 0, so the explicit assertion and a structural policy test in
`tests/test_release_workflow.py` guard execution. The extension wrapper starts
its normal backend container for this second invocation; it reuses the
existing harness, not a live service from the earlier step. Closure-gap lesson
recorded in `docs/lessons.md` (dated):
*a closed plan's unexecuted task with no deviation row is invisible
debt; closure review diffs tasks against evidence, not the
checklist.*

### J. Remaining docs/truthfulness batch (rebuilt per review)

Per-unit public docs and CHANGELOG entries now land **in their owning unit's
commit** (A, B, and E carry their own texts), so J is no longer a coupled
catch-all. Unit E owns cleanup help, README, spec registry, agent kernel,
`llms.txt`, and release-note alignment. J's remaining content, sequenced after
the final unit set is known:
- `docs/guides/configuration.md:~300–301`: replace the paraphrased
  project-scope error with the verbatim stderr text.
- CHANGELOG 6.0.2 **Documented** entry recording the 6.0.0 removal of
  the importable `simplebroker.helpers` module.
- `docs/plans/README.md`: correct the stale
  `2026-08-06-access-backend-benchmark-plan` row ("awaiting owner
  commit" — owner commit `829b032` landed).

### K0. Release mechanics decision (F10 plus round-2 owner ratification)

`bin/release.py` is the executable authority, and it contradicts
revision 1 on every disputed point: **batch target order is pg → redis →
core** (:374–378); tags are pushed together without inter-package
publication waits (:2065–2107); standalone core **refuses** unpublished
extension baselines (:1899–1915, :2465–2467); PyPI publication precedes the final GitHub
release; published versions are immutable. Revision 1's "core first"
answer and its "failed publish leaves PyPI at the old coherent state"
rollback claim are both withdrawn.

K0 decisions (normative for K):

1. **Publish via the existing batch mode from one green SHA** (precedent:
   2026-07-31 release from `926ae54f`). The driver pushes tags in
   pg → redis → core order. The three tag-triggered workflows then run
   independently; workflow and PyPI completion order is unspecified. This
   current asynchronous mechanism is owner-ratified and is not changed by the
   plan.
2. **Accepted transient**: while the three independent workflows complete,
   any subset of the new package versions may be visible. Some new-version
   extension/extras pairings are therefore temporarily uninstallable until
   their counterpart appears. The exact old versions (6.0.1/3.5.0) remain
   available; no claim is made about which new package appears first or how
   long the transient lasts.
3. **Post-publish verification**: inspect all three workflow runs for the exact
   tag and SHA, wait for terminal success and PyPI visibility of all
   three; then clean-venv installs of `simplebroker==6.0.2`,
   `simplebroker[pg]==6.0.2`, `simplebroker[redis]==6.0.2`; verify
   tag SHAs match the green SHA and package metadata floors.
4. **Recovery is per tag/workflow, not by publication prefix:** if a remote tag
   is absent, inspect state and rerun the batch so the missing tag can be
   pushed. If the exact remote tag exists and its workflow failed, fix the
   cause and rerun that existing workflow for the same tag/SHA; pushing the
   same tag or merely rerunning batch does not retrigger it. If a tag exists
   but no corresponding workflow run exists, stop and investigate rather than
   moving or force-pushing the tag. A last-resort corrective package version
   requires a new plan.
5. **No statement anywhere in this plan claims rollback after any
   PyPI publish.**

### Deferred units register (owner-ratified; reopen conditions named)

| Unit | Finding | Why deferred | Reopens when |
|---|---|---|---|
| C — config error hygiene | F3: the CLI catch is unreachable — eager package/module config loads at import (`__init__.py:7`, `_broker_session.py:19`, `cli.py:40,1519`), and Weft's `resolve_config(overrides)` path needs its own diagnostics | A `main()`-level catch cannot intercept import-time failure; the real fix is a bootstrap/eager-config redesign | A dedicated plan specifies lazy/staged config or import-safe diagnostics across `broker`, `python -m simplebroker`, library import, and Weft's override path |
| D — `.broker.toml` provenance notice | F4: unconditional stderr commentary contradicts [SB-CLI-2]; the `--status`-already-reports-provenance premise was false (status emits metrics); this is CLI UX policy, not debt | Public CLI contract change requiring an owner UX decision plus a full command/quiet/JSON/source-precedence matrix and spec delta | Owner chooses the UX; a [SB-CLI-2]-consistent spec delta with the full matrix accompanies it |
| F — `remove_alias` hook symmetry | F6: the "symmetry" fix creates a **PostgreSQL lock-order cycle** — add/remove would take alias advisory lock → meta row while rename takes meta row → advisory lock (`db.py:3017–3049`, pg `plugin.py:706–739`); SQLite supplies a no-op hook, so "no hook/byte-identical" was also false | A one-line fix is unsafe; the real slice is one global lock order, a backend-API compatibility decision, and a forceable two-connection deadlock test | A dedicated pg lock-order slice with bounded-completion deadlock tests in both acquisition orders |
| G — watcher GC finalizer | F7: `Thread(target=self.run_forever)` strongly retains the watcher, so GC-time cleanup for a live watcher is **structurally impossible**; extracting strong refs bypasses owner-thread shutdown ordering; theory-bearing per [REV-THEORY-005] | The truthful near-term contract is explicit close/context management; the GC promise cannot be patched into truth | A dedicated watcher-lifecycle redesign (non-bound thread target, lifecycle transitions, cross-thread rules) grounded in program theory; or a docs-only truthfulness fix landing the honest contract wording |
| H — redis conflict retry | F8: the adjacent `_write_lock` change **self-deadlocks patterned broadcast** (`broadcast()` delegates to `insert_messages()` under a non-reentrant `threading.Lock`, `core.py:72–90,1498–1678`); the unit conflated four write paths; exact-ID insert must stay immediate per [SB-ID-4]; the seam is `.eval`, not `evalsha`. **Supersedes the earlier owner direction to apply `_retry` here** — the direction stands in spirit (budgeted, not attempt-capped) but requires the registered state-machine redesign, not a drop-in | No release-blocking contention evidence exists; a registered-state-machine change (implementation docs 07/08) is not pre-release hygiene | Measured contention evidence, or a dedicated plan redesigning from the registered write/broadcast state machines with per-result-code transition preservation, reentrancy-safe lock boundary, fake-clock `.eval` units, and real Valkey contention tests |

### Comprehension questions (answer before editing)

1. Why are there three timestamp grammars rather than one gate, and how is
   the no-fractions rule applied? (Expected: suffix parsing precedes
   unsuffixed classification, so both numeric paths need integer-only gates;
   ISO has its own grammar with `+`/`-`/`:` but must separately reject a
   fractional-second component. Integer `ms`/`ns` and native IDs provide
   finer granularity.)
2. What is the SQLite cross-user filesystem boundary? (Expected: every
   intended writer needs read/write access to the main database and all listed
   SQLite/SimpleBroker companions, plus write and execute/search access to the
   containing directory. SimpleBroker does not provision or preserve a
   group-sharing policy, and this release adds no forced-mode helper.)
3. Why is revision 3's cleanup protocol rejected, and what replaces it?
   (Expected: checkpointing
   after `BEGIN EXCLUSIVE` fails even without contention; in WAL mode that SQL
   transaction does not exclude readers or prove last-connection status; and
   SQLite documents unlinking an open database followed by replacement at the
   same pathname as undefined and potentially corrupting. Revision 5 does not
   try to make deletion safe: `--cleanup` explicitly attempts the exact
   main/`-wal`/`-shm` namespace and puts all overlapping SQLite storage effects
   in the documented undefined blast radius. Only the CLI's attempt/result
   semantics remain defined.)
4. Which order is deterministic in release, and which is not? (Expected:
   remote tags are pushed pg → redis → core; the three workflows and PyPI
   publications complete independently in unspecified order. Any temporary
   subset of new packages is accepted. Recovery reruns a failed existing
   workflow for its immutable tag/SHA; it does not claim rollback.)

## Invariants and Constraints

1. **No accepted-input widening anywhere.** Unit A narrows underscore/sign
   pseudo-numerics and every fractional-second spelling. Integral ISO forms,
   integral numeric forms, and [SB-ID-4] exact-ID acceptance are unchanged.
   Weft has no fractional dependency; T-A keeps only a bounded smoke matrix
   for retained Weft integer/ISO paths.
2. **[SB-CLI-1]/[SB-CLI-2] discipline holds**: Unit A rejections use
   one actionable bound-parse error path and the existing exit codes; no
   unit adds stdout/stderr commentary (the unit that would have — D —
   is deferred for exactly that reason).
3. **Unit E is destructive and non-atomic.** It validates an existing main
   database before any deletion, then attempts only exact `-wal`, `-shm`, and
   main names. It never opens SQLite, checkpoints, waits for clients, or claims
   quiescence. Partial deletions are irreversible; errors name every failed
   path. Concurrent SQLite storage outcomes are undefined, while CLI exit and
   stream behavior remain [SB-CLI-1]/[SB-CLI-2] contracts.
4. **Unit B is documentation-only.** No exact file mode is a release invariant
   and no code path creates, normalizes, or repairs a group-sharing policy. The
   documented condition covers the main database, the complete associated-file
   list, and directory write plus execute/search access. Existing mode behavior
   is unchanged.
5. **No poisoning-machinery changes** (Unit I is CI-only), and no
   redis, pg-extension, watcher, or config-bootstrap code changes
   anywhere in this train (those units are deferred).
6. **No drive-by refactors.** Each unit touches only its named files.
7. **Release gate (K)**: nothing is tagged or published until the
   full Verification block is green from the release SHA; the publish
   uses K0's existing batch tag path; **no rollback claims after any PyPI
   publish** — recovery is per immutable tag/workflow as K0 states.
8. **Deferred ≠ dropped**: C, D, F, G, H keep their register rows
   here and their findings remain pinned in the maintainer ledger;
   any of them landing later starts from its reopen condition, not
   from revision 1's rejected task text.

## Hidden Couplings

- Unit A's three-grammar contract spans the CLI (`--after`/
  `--before`), dump/load exact selectors, watchers, and Weft's direct
  parser use — one choke point, four consumers; tests cover each.
- Unit B's documentation must describe one namespace, not only the main/WAL/SHM
  trio: SQLite journals and SimpleBroker lock/status/vacuum files can all gate a
  second OS user. The text is a deployment condition, not an automatic support
  guarantee.
- Unit E belongs to the backend plugin. Its exact-name deletion attempts are
  coupled to the existing boolean backend result and the CLI quiet/stream
  mapping. It intentionally has no connection-lifetime coordination. The spec
  must not turn diagnostic probe outcomes into promises.
- Unit I's three workflows have different runners and env prerequisites; each
  pg/redis wrapper invocation owns a normal container lifecycle. Reuse the
  existing harness, not an assumed live service from an earlier step.
- K0's accepted transient interacts with J's CHANGELOG wording: the
  release notes must not promise atomic availability.

## Rollback and Rollout

- Software commits for A, E, I, and J are independently revertible before
  publish. B is documentation inside J and changes no permissions. E's
  deletions are one-way doors: a code revert cannot restore deleted data. A and
  E are the two normative contract deltas.
- Rollout: land A/E/I in any order → J (including B's limitation) → full gates
  green → K0 checklist → batch publish per K. **After
  any PyPI publish there is no rollback**; K0's per-workflow recovery applies.
- If any retained unit stalls in re-review, it may be dropped from
  the train EXCEPT A (contract truth) and K (the release itself);
  drops get index-row notes.

## Proposed Spec Deltas (strategy B — atomic, one per owning unit)

### Delta 1 — `docs/specs/10-cli.md` [SB-CLI-5] (lands with Unit A)

Replace the sentence "Digits in these forms may be any Unicode
decimal digits (`str.isdecimal()`)." with:

> Digits in these forms may be any Unicode decimal digits. Bound
> strings parse under exactly three grammars, applied after
> whitespace stripping and digit folding: (1) ISO-8601, per that
> grammar but without a fractional-second component; (2) unsuffixed
> numeric, where the entire candidate must
> satisfy `str.isdecimal()` before unit classification — underscore
> separators, sign prefixes, and other characters `int()` tolerates
> are rejected rather than silently changing the unit
> classification; (3) suffixed numeric (`<digits><unit>`), where the
> complete number portion must satisfy `str.isdecimal()` under the same
> rejection rule. Fractional seconds are unsupported in every grammar.
> Use integer `ms`, integer `ns`, or a native hybrid message ID for finer
> granularity. A string failing all three grammars is rejected with an
> actionable bound-parse error that states this limitation.

Unit A CHANGELOG (6.0.2, Fixed — lands with A):
- Timestamp bounds now use integral seconds only. Fractional components in
  bare numeric, suffixed numeric, and ISO-8601 spellings are rejected with
  guidance to use integer `ms`, integer `ns`, or a native hybrid message ID.
  Numeric bounds also require decimal-digit-only spellings; previously
  `int()`-tolerated forms such as `1_705_329_000` or `+99999999999` could
  select the wrong unit because separators were counted as digits.

### Delta 2 — `docs/specs/17-ops.md` [SB-OPS-7] destructive target cleanup
(lands with Unit E)

> Global `--cleanup` is an explicitly destructive request to delete the
> configured backend target state and exit. It is not a backup, rollback, or
> quiescent-maintenance operation. Backend-specific effects are authoritative;
> CLI exit codes and streams follow [SB-CLI-1]/[SB-CLI-2].
>
> SQLite `:memory:` and empty targets have no owned filesystem namespace;
> cleanup is a successful no-op and derives no filenames for them. For a
> SQLite filesystem target, one expanded, resolved path is frozen for
> validation and all owned-name derivation.
>
> For a SQLite filesystem target, cleanup owns the complete known
> SimpleBroker filesystem namespace: the resolved configured main path; names
> formed by appending `-journal`, `-wal`, `-shm`, `.lock`, `.status`, and
> `.vacuum.lock`;
> and crash-residue entries matching
> `.status.tmp.<decimal-pid>.<decimal-time_ns>` in the same directory. Both
> variable components are nonempty ASCII decimal digits. No other prefix, glob,
> or recursive scan is authorized.
> Path derivation and main-path inspection are a zero-delete ownership
> preflight: an inspection error aborts before mutation. Status-temp enumeration
> is best-effort; a missing parent is empty, while any other enumeration error
> is recorded, every fixed owned name is still attempted, and the command later
> reports the error. Matching temp entries yielded before a mid-iteration error
> remain candidates and are attempted in lexical order. If the main path
> exists, it must validate as an initialized SimpleBroker
> database before any owned entry is unlinked; failed validation leaves the
> whole namespace untouched. If the main path is absent, the explicit
> destructive request may still unlink the complete orphan namespace. An owned
> entry that is a symlink, including a dangling symlink, is counted and unlinked
> without following it.
>
> After validation, cleanup attempts `.status.tmp.*` residues, `.status`,
> `.vacuum.lock`, `.lock`, `-journal`, `-wal`, `-shm`, then the main path.
> Fixed names are unlinked directly: success means found and `FileNotFoundError`
> means absent. A main or temp entry observed earlier still counts as found if
> it disappears before unlink. Cleanup attempts every candidate even
> after a prior failure; a partial result is possible and irreversible. Exit `0`
> means enumeration succeeded and every candidate was absent or unlinked. After
> all possible attempts, an enumeration or unlink failure produces one nonzero
> operational-error result and one stderr diagnostic naming the failed attempts
> and stating that other entries may already be gone. That is the only response:
> cleanup does not retry or roll back. `--quiet` suppresses success/no-op status
> but not this error. No result
> promises that a concurrent process will not recreate a deleted name.
>
> Apart from short-lived read-only ownership validation when the main file
> exists, cleanup does not open SQLite, checkpoint, or wait for other
> connections. If any active SimpleBroker operation/process using the target or
> any raw SQLite connection overlaps cleanup,
> **the exact storage, coordination, and client outcomes are undefined**. This
> includes durability and visibility of old or
> new writes, which database generation a client observes, whether an operation
> errors or appears to succeed, whether any owned names reappear, whether
> generations interfere, and when unlinked disk space is reclaimed. This is the
> SQLite upstream boundary for unlinking an open database on Unix, not a
> SimpleBroker concurrency guarantee. Active SimpleBroker setup, phaselock,
> status-publication, and vacuum operations are also undefined overlap because
> deleting a held lock path can split coordination across old and replacement
> inodes. Concurrent directory-entry replacement is likewise outside the
> validation guarantee. On Windows and other systems that refuse
> deletion of an open entry, those failures follow the same aggregate error
> contract; earlier successful deletions are not rolled back. Operators who
> need a predictable result must stop all SimpleBroker activity and raw SQLite
> clients before cleanup and must make any required backup before invoking it.

Unit E CHANGELOG (6.0.2, Fixed — lands with E):
- SQLite `--cleanup` now attempts the configured main database plus its
  `-journal`, `-wal`, `-shm`, `.lock`, `.status`,
  `.status.tmp.<pid>.<time_ns>`, and `.vacuum.lock`
  state. It remains deliberately destructive and non-atomic. Concurrent SQLite
  storage outcomes are undefined per SQLite upstream, while validation, exit,
  quiet, and partial-failure diagnostics remain defined.

### Unit B documentation contract (lands with J; no normative API delta)

> For access by more than one OS user, the filesystem must grant every intended
> writer effective read and write access to the broker database and every
> associated file that exists or may be created: `<db>-journal`, `<db>-wal`,
> `<db>-shm`, `<db>.lock`,
> `<db>.status`, `<db>.status.tmp.<pid>.<time_ns>`, and `<db>.vacuum.lock`.
> The containing directory must let every intended writer traverse it and
> create, replace, and remove those entries. On POSIX this requires appropriate
> directory write and execute/search permission; on Windows it requires the
> equivalent effective file/directory ACL rights. SimpleBroker does not promise
> to provision or preserve a group-sharing policy. Operators own directory
> placement, ownership, groups/principals, ACLs, existing permissions, and (on
> POSIX) the process umask. For sensitive broker contents, use a directory
> restricted to the intended users. Do not rely on a uniform exact permission
> set across all artifacts.

Unit B CHANGELOG (6.0.2, Documented — lands with J):
- Clarified the SQLite filesystem access boundary: cross-user deployments must
  grant every intended writer access to the main database, all SQLite and
  SimpleBroker companion files, and the containing directory on POSIX and
  Windows. SimpleBroker does not automatically provision or preserve a
  group-sharing policy.

(J's Documented entry for the 6.0.0 `simplebroker.helpers` removal is
unchanged from revision 1 and lands with J.)

## Tasks

Plan-remediation order: correct the conflicting durable lesson → implementation
A/E/I → J (including B's permission limitation) → K0 → K. The durable
lesson correction and owner authorization have cleared the implementation gate.

**T-A. Timestamp grammar (F1-revised)** — files:
`simplebroker/_timestamp.py`, `simplebroker/cli.py`, `README.md`,
`docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md` [SB-API-11],
`tests/test_after_flag.py`, `tests/test_timestamp_edge_cases.py`, and
`tests/test_timestamp_bound_grammar.py` (new). Implementation point: apply
the integral rule independently to ISO, bare numeric, and suffix-first
numeric paths. Existing acceptance tests for bare `"1.532"`, suffixed
`"1705329000.5s"`, fractional milliseconds, high-precision fractions, and
ISO fractional seconds become rejection tests. Also reject
`"1_705_329_000"` and `"+99999999999"`. Positive controls include integral
seconds, integer `ms`, integer `ns`, ISO with an offset and integral seconds,
non-ASCII decimal digits, and exact hybrid IDs. Direct public
`TimestampGenerator.validate()` and real CLI `--after`/`--before` tests must
assert the actionable error and unchanged exit/stream behavior. README and
CLI help teach the limitation and alternatives. The public API spec either
owns the validator grammar or normatively delegates it to [SB-CLI-5]. Weft
gets a bounded retained-form smoke matrix only; the owner has confirmed no
fractional dependency. Spec Delta 1 + API link/delta + Unit A CHANGELOG land
in the same commit.

**T-B. SQLite permission limitation (revision 6; documentation-only, lands in
T-J)** — files: `docs/guides/configuration.md` (:489–497 security section),
README security/link wording as needed, and `CHANGELOG.md`. Remove the false
uniform-`0600` claim. Add the exact documentation contract above with the main
database plus `-journal`, `-wal`, `-shm`, `.lock`, `.status`,
`.status.tmp.<pid>.<time_ns>`, and `.vacuum.lock`; require read/write permission
on every file for every intended writer and permission to traverse plus create,
replace, and remove entries in the containing directory. Name POSIX directory
write plus execute/search permission and Windows file/directory ACLs as the
platform forms. State that SimpleBroker does not provision or preserve group
sharing and that sensitive brokers belong in a directory restricted to the
intended users (with a suitable umask on POSIX). No product code,
new test module, config key, API delta, exact-mode assertion, or mode-repair
behavior lands.

**T-E. Destructive SQLite cleanup (revision 5)** — files:
`simplebroker/_backends/sqlite/plugin.py`,
`simplebroker/_backends/sqlite/validation.py`,
`simplebroker/_backends/sqlite/maintenance.py`, `simplebroker/_runner.py`,
`simplebroker/_phaselock.py`, `simplebroker/cli.py`,
`docs/specs/17-ops.md` [SB-OPS-7], `docs/specs/product-section-registry.md`,
`docs/specs/00-specs-index.md`, `docs/agent-kernel.md`, `llms.txt`, `README.md`,
`CHANGELOG.md`, `tests/test_cleanup.py`, `tests/test_cli_argument_parsing.py`,
`tests/test_operations_contract_sb_ops.py`, and a focused multiprocess
diagnostic probe if retained. Implement Context E's closed owned-namespace
state machine without a read-write/lifecycle SQLite connection or a new backend
result type. Required firing cases: (1) valid main plus journal/WAL/SHM/lock/
status/vacuum-lock/status-temp residue all removed; (2) invalid, foreign,
unreadable, or directory main causes zero deletion; (3) absent main plus owned orphan entries
removes those entries and reports cleanup rather than no-op; (4) all absent is
idempotent success/no-op, including a missing parent with no directory created;
(5) an injected failure for each position proves
later paths are still attempted, exit is nonzero, and every failed attempt is
named; a two-failure case proves aggregation and ordering;
(6) live and dangling owned-name symlinks are unlinked without touching their
targets; (7) near-miss files outside the allowlist survive, including
`.status.tmp`, `.status.tmp.123`, `.status.tmp.x.1`,
`.status.tmp.1.2.backup`, `.status.tmp.١.٢`, `.lock.backup`, and
`-wal.backup`; (8) observed-main and observed-temp disappearance races succeed
and still report work found, while direct fixed-name `ENOENT` is absent; (9)
`--quiet` suppresses only success/no-op;
(10) Windows/open-handle refusal uses a clean nonzero error with no traceback
and no rollback claim; (11) backend/validation tests for real main filenames
containing `?`, `#`, and `%` delete the intended target only, while the CLI
case covers `%` and retains its existing rejection of unsafe `?`/`#` path
spellings; (12) `:memory:` and empty targets
are no-ops and do not derive or delete same-spelled filesystem entries; (13)
resolved-symlink targeting deletes the actual database namespace, not a mixture
of alias-side and target-side names; (14) JSON error mode preserves its
structured [SB-CLI-2] envelope; (15) injected main-`lstat` failure proves the
zero-delete validation gate; injected status-temp enumeration failure proves
that every fixed name is still attempted before the nonzero report, and a
mid-iteration failure proves already-yielded matching temps are sorted and
attempted; (16) an
enumerated entry that disappears counts as work found while a never-observed
absent fixed sidecar does not; (17) multiple matching status temps are attempted
and diagnosed in lexical order. POSIX old-client/new-client and
held-lock/replacement-lock probes may
record observed results, but they must not assert a particular
SQLite outcome or gate correctness on one because [SB-OPS-7] makes that branch
undefined. Delta 2 + Unit E CHANGELOG land atomically.

The same slice changes global help to “destructively delete configured backend
target state and exit”; updates README cleanup examples and every
`[SB-OPS-1]`–`[SB-OPS-6]` inventory to include `[SB-OPS-7]`; adds cleanup to
the registry and agent-kernel operations map; and adds an exact executable
evidence manifest/verification row for `[SB-OPS-7]`. SQLite detail names the
complete owned namespace and says stopping all clients is the way to avoid the
undefined overlap case, not a condition cleanup itself enforces.
The runner, phaselock, and vacuum-lock comments retain their ordinary-lifecycle
rule (individual handles and maintenance passes never unlink shared lock state)
but name explicit destructive `--cleanup` as the sole exception and warn that
overlap can split lock generations.

**T-I. Probe wiring, three workflows (F9-revised)** — files:
`.github/workflows/test.yml`, `.github/workflows/test-pg-extension.yml`,
`.github/workflows/test-redis-extension.yml`,
`tests/test_release_workflow.py`, and `docs/lessons.md`. One explicit probe
step per workflow (Context I), non-coverage and conditioned to one Linux/Python
matrix member. Each step must assert the opt-in variable equals `1` before
pytest. A structural test enumerates the three workflows and proves exactly
one conditioned direct probe invocation, the exact environment value, and the
expected probe path in each. Local verification runs each probe file once with
the env var set (root directly; pg/redis through `bin/pytest-pg` and
`bin/pytest-redis`, each using its own normal container lifecycle). CI proof
lands with the push. Exit 5 is documented only as the collection guard; the
environment assertion is the execution guard.

**T-J. Remaining docs batch (rebuilt)** — files:
`docs/guides/configuration.md:~300`, `CHANGELOG.md` (helpers.py Documented
entry; retain each owning unit's entries), and `docs/plans/README.md` (benchmark row).
Verification by inspection,
`bin/check-dom15-fixtures`, and `bin/check-plan-context`. Sequenced after A/E/I
so the CHANGELOG reflects the actual final set.

**K0. Release-mechanics checklist (F10)** — no code; execute the
decisions in Context K0 as a written checklist in this plan at release time:
batch tag-order confirmation, green-SHA capture, identification of the three
tag-triggered workflow runs, bounded waiting for terminal results, the three
post-publish verifications, and per-workflow recovery on standby. Do not infer
publication order from tag order and do not redesign the accepted mechanism.

**T-K. Release** — push main; confirm full CI green on the exact SHA
(including the three new probe steps); run `bin/release.py` batch
mode (pg 3.5.1 → redis 3.5.1 → core 6.0.2); execute K0 step 3
verification; flip this plan's index row per the completion gate.
**Stop gates:** any red in the Verification block stops the publish;
any deviation from K0's order or checks stops and records before
proceeding. **Possession probe (theory discipline):** at this
release, run one possession probe and record its outcome in this
plan before the index row flips — first administration of the
instrument added 2026-08-06.

## Testing Plan

- Harness: root pytest (`uv run pytest -n0` targeted; full for
  gates), `bin/pytest-pg --fast` / `bin/pytest-redis --fast` for the
  extension gates (unchanged code, still gated), timing helpers as
  house standard.
- **Real:** SQLite DBs and real filesystem entries for cleanup, plus real CLI
  processes for `--after`/`--before` and `--cleanup`. Unit B adds no mode
  behavior or mode test; its documentation is checked for the complete file and
  directory-access boundary. The undefined concurrent-cleanup branch
  gets a diagnostic multiprocess probe, not an expected-outcome gate.
  **Sanctioned doubles:** narrow `lstat`, directory-enumeration, and
  unlink-failure injection for Unit E's otherwise unforceable observation and
  ordered partial-failure matrix; units F, G, and H are deferred.
- Contract focus: actionable error texts and exit codes, the documented
  permission boundary, the
  exact cleanup namespace and ordered attempt matrix, the focused CLI-help
  assertion plus truthful README/spec/CHANGELOG undefined-concurrency wording,
  instant-selection equivalence across integral spellings, and bounded Weft
  retained-form smoke coverage.
- Red-first throughout; every red above names its failing form and
  must fail on unmodified code (or be converted to a pinned control
  where current behavior is already correct, stated explicitly).

## Verification and Gates

Per-task: the unit's named tests green. Final (rerun from the release
SHA; a prior green is not evidence):

```bash
uv run pytest -n0 -q
PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= uv run pytest -n0 tests/test_cross_thread_generator_probe.py -q
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py
uv run --frozen --no-sync ruff check simplebroker tests
uv run --frozen --no-sync ruff format --check simplebroker tests
uv run --frozen --no-sync mypy simplebroker --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
git diff --check
```

Post-publish signals: K0 step-3 clean-venv installs pass; fractional-bound
rejections match the disclosed limitation; SQLite permission docs name the
complete owned namespace and directory boundary without an exact-mode promise;
the three probe
steps visible in their workflows' logs; SQLite cleanup removes the full known
owned namespace without claiming a concurrent-client outcome; the
possession-probe outcome recorded.

## Independent Review Loop (class 5)

- Round 1 (2026-08-06, revision 1): **BLOCKED** — F1–F10; full report
  below; all ten dispositioned in `## Finding Dispositions
  (Revision 2)`.
- Round 2 reviewer: same family assignment (Codex primary; focused
  passes as round 1). Inputs: this revision including the disposition
  table and all three spec deltas; `_timestamp.py` suffix/order code;
  `_runner.py:333–343`; the SQLite plugin/maintenance cleanup owner;
  the three workflow files; `bin/release.py` batch mode and its
  tests; Weft's `queue.py:936`.
- Round 2 stance: verify each disposition against code — especially
  (1) the three-grammar contract vs the actual parse order and Weft's
  inputs; (2) runner-level create/normalize vs the eight-process race
  gate and the belt interaction; (3) the cleanup state machine's
  validation and ordering against the plugin's existing lifecycle;
  (4) the three probe steps against each workflow's real service
  setup; (5) K0 against `bin/release.py`'s actual batch behavior and
  refusal paths. Confirm the five deferrals are cleanly severed (no
  retained task depends on deferred code). Explicit
  could-you-implement verdict required.
- Round 2 completed BLOCKED on B/E. Round 3 also completed BLOCKED. Revision 4
  deferred E. Revision 5 supersedes that deferral with the deliberately
  destructive [SB-OPS-7] contract after targeted POSIX/Linux/macOS probes and
  agent-facing interface and independent lifecycle passes. Implementation
  was later followed by revision 6's owner decision to drop B's forced-mode
  implementation after deployment/threat-model review. The durable-lesson
  correction and 2026-08-07 owner direction cleared that final blocker. The
  completed-work review initially found three timestamp gaps, all corrected;
  its focused re-review passed on 2026-08-07. T-K still requires its own
  release-time gates.
- Round 3 (2026-08-06, revision 3): **BLOCKED**. Unit B's shared helper
  direction is viable but its exact mode, existing-target authority, symlink
  identity, compatibility, race, and Windows contracts remain incomplete.
  Unit E's literal protocol is not executable and its open-file unlink is an
  SQLite-documented corruption pattern. See the round-3 findings below.
- Revision 4 owner disposition is historical. Revision 5 keeps E without its
  disproved SQL protocol: exact destructive deletion, no quiescence, and
  undefined concurrent SQLite storage results. Revision 6 supersedes the open
  B findings by removing the proposed mode behavior and retaining only the
  explicit operator-owned permission limitation.

## Verified Implementation Record (2026-08-07, initial implementation)

Scope implemented: A (integral-only timestamp-bound grammar), B/J
(operator-owned cross-platform permission limitation and remaining truthfulness
docs), E (destructive best-effort SQLite cleanup), and I (three owning CI probe
steps). C/D/F/G/H remain deferred. K0/K were not executed.

The timestamp simplification removed both remaining C901 findings in the parser
family. `[RUFF-SUP-013]` was therefore retired without renumbering later stable
group IDs; the registry, generated index, global raw inventory, and policy gate
now bind the smaller live set.

### Completed-work independent findings

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-F1 | P1 | `simplebroker/_timestamp.py:42–45,514–517`; `tests/test_timestamp_bound_grammar.py:50–64,112–131` | Python's ISO parser also accepted fractional *offset* seconds, outside the integral-only grammar. | **Resolved.** Reject any decimal fraction after the ISO date/time separator before `fromisoformat`; public and CLI cases cover dot/comma offsets. |
| IR-F2 | P2 | `simplebroker/_timestamp.py:362–367`; `tests/test_timestamp_bound_grammar.py:79–90,134–147` | Scientific-notation rejection bypassed the required finer-grain recovery guidance. | **Resolved.** Retain the rejection and append integer-ms, integer-ns, and native-ID guidance; bind the JSON error path. |
| IR-F3 | P2 | `docs/specs/10-cli.md:165–175`; `tests/test_cli_contract_sb_cli.py:198–221` | [SB-CLI-5] named files but had no exact executable evidence manifest. | **Resolved.** Add the ten-node manifest and AST-backed conservation gate. |

Independent re-review verdict: **PASS; no blocker.** It also rechecked Unit E's
bounded authority, ordering, best-effort aggregation, path handling, and
Windows refusal case; Unit I's owning-workflow conditions; and Unit B's full
file-plus-directory permission boundary. No files were edited by the reviewer.

### Agent-facing interface implementation review (CLI)

| Principle | Result and evidence |
|-----------|---------------------|
| 1. Context is the scarcest resource | Met: timestamp failures carry one compact recovery sentence (`simplebroker/_timestamp.py:46–49`); cleanup emits one aggregate diagnostic rather than a state dump (`simplebroker/_backends/sqlite/plugin.py:80–89`). |
| 2. Progressive disclosure | Met: `--help` states the limit/destructive action (`simplebroker/cli.py:131–145,210–214`), README gives operational guidance (`README.md:188–212,330–344`), and [SB-CLI-5]/[SB-OPS-7] own exact detail. |
| 3. Self-explanatory names | Met: `--after`, `--before`, and `--cleanup` retain direct names; help describes effects and alternatives at the flags (`simplebroker/cli.py:131–145,210–214`). |
| 4. One identity per thing | Met: cleanup freezes one expanded/resolved target and derives every owned name from it (`simplebroker/_backends/sqlite/plugin.py:92–102,230–247`); all bound strings enter `TimestampGenerator.validate()` (`simplebroker/_timestamp.py:307–389`). |
| 5. Derive what is derivable | Met: the caller supplies one target and cleanup derives the closed namespace (`simplebroker/_backends/sqlite/plugin.py:238–248`); the parser derives units only from the documented spelling (`simplebroker/_timestamp.py:369–489`). |
| 6. No hidden session setup | Met: both operations are single invocations. CLI target context is explicit or documented ambient state (`README.md:200–212`); cleanup requires no prior marker, xattr, or quiescence session (`docs/specs/17-ops.md:175–185`). |
| 7. Teach, do not reject | Met with a safety boundary: unsupported fractional/scientific forms are real grammar conflicts but errors teach integer-ms/ns/native alternatives (`simplebroker/_timestamp.py:362–367,514–517`); a foreign main is a destructive ownership conflict and fails before mutation (`simplebroker/_backends/sqlite/plugin.py:234–239`). |
| 8. Every message carries its action | Met under the ratified policy: bound errors state the usable alternative; cleanup names every failed attempt and the irreversible partial-result condition (`simplebroker/_backends/sqlite/plugin.py:80–89`). |
| 9. Atomic writes with recovery | Deliberate documented departure for cleanup: multi-file deletion is non-atomic, irreversible, and has no rollback. Stop activity and back up first (`README.md:209–212`; `docs/specs/17-ops.md:207–236`). Timestamp parsing is read-only; the multi-writer merge clause is not applicable. |
| 10. Draw the trust boundary | Met: an existing main must validate before deletion, absent-main authority is limited to an exact allowlist, and no recursive scan is permitted (`docs/specs/17-ops.md:187–205`). Cross-user filesystem policy is explicitly operator-owned on POSIX and Windows (`docs/guides/configuration.md:494–520`). |
| 11. Wire format matches the mental model | Met: the CLI speaks in timestamp bounds and configured target state; SQLite generation and companion-file details live in the deeper limitation/spec (`README.md:209–212,330–344`; `docs/specs/17-ops.md:220–236`). |

Findings verdict: **no blocker**. Ratified judgments: fractional seconds remain
unsupported rather than silently normalized; cleanup's non-atomic destructive
behavior is explicit rather than disguised as maintenance; cross-user
permissions remain operator policy rather than a forced universal mode.
Runbook feedback: **no new reusable principle candidate**; the two review-found
timestamp gaps are local examples of existing principles 7, 8, and the
enumerable-contract gate.

### Verification evidence

- Root: `uv run pytest -n0 -q` passed on 2,635 collected tests (2,617 passed;
  18 platform/service/opt-in skips).
- PostgreSQL wrapper: 1,139 shared tests and 175 extension tests passed; eight
  expected skips. One unrelated watcher-SIGINT xdist run needed SIGKILL; the
  exact node passed immediately and the complete wrapper rerun then passed.
- Redis wrapper: 1,131 shared tests and 246 extension tests passed; twelve
  expected skips.
- Explicit finalization probes: SQLite 4, PostgreSQL 5, Redis 1 passed with
  `SIMPLEBROKER_RUN_FINALIZATION_PROBE=1` through their owning invocations.
- Ruff check/format, mypy (43 source files), suppression-index check, DOM-15
  fixtures, plan-context, and `git diff --check` passed.
- Local residual: the real Windows open-handle cleanup firing test is skipped
  on macOS and remains a Windows CI obligation. The undefined concurrent
  SQLite outcome is deliberately not asserted. No release, publish, or
  possession probe was run.

### First-run CI remediation (2026-08-07)

The first exact-SHA run for `a38e6a9`, GitHub Actions run
[`31184958528`](https://github.com/VanL/simplebroker/actions/runs/31184958528),
separated deterministic portability defects from two Windows scheduling
failures.

Deterministic classification and fixes:

- **Test error:** `test_cleanup_general_error` mocked global `Path.exists()`
  after cleanup ownership moved to the SQLite backend adapter. An ignored
  repository-root `.broker.db` made the stale test pass locally while clean CI
  correctly returned the absent-target no-op. The test now uses an isolated
  target and injects the error at `SQLiteBackendPlugin.cleanup_target()`.
- **Test error:** the aggregate-failure fixture assumed `.status` did not exist,
  although the real write can leave that owned coordination entry. The fixture
  now removes the file before replacing it with the directory used to force a
  portable unlink refusal.
- **Test portability error:** `?` is not a legal Windows filename. The literal
  URI-metacharacter proof skips only that spelling on Windows; `#` and `%`
  continue to exercise the portable backend validation boundary.
- **Application error:** Python 3.13/3.14 on Windows allowed an embedded-NUL
  path through `Path.resolve()` and raised `ValueError` later at `lstat()`.
  The cleanup observation gate now translates both `OSError` and `ValueError`
  to the promised zero-delete `DatabaseError`.

The intermittent failures are classified as test/CI topology errors unless a
serial Windows rerun produces contrary evidence. On Windows 3.11, the four
xdist workers were occupied by one invalid-filename phase-lock retry and three
resilient-worker tests that each launched a fresh CLI process for their first
SQLite write; all three child commands exhausted the 24-second CI helper
budget on isolated targets. No changed production write path explains that
cluster. The tests now seed through one backend-native `Queue` connection and
retain CLI calls only for the checkpoint behavior under test; the resilient
module also shares one xdist group.

On Windows 3.13, `test_streaming_read_all` ended as `node down: Not properly
terminated`. The leading explanation is the known `pytest-timeout` thread
mechanism, not a native crash: the test has a 360-second marker and the
installed timer ends the worker with `os._exit(1)`. The quiet xdist log did not
print the test's exact start time, so the signature alone is not conclusive;
the same signature did occur at the measured 180-second boundary in run
`30658218016`. The two preserved
1,000-message streaming proofs now run in a dedicated `-n0` Windows phase;
the normal Windows xdist phase excludes their marker. Every xdist matrix
invocation disables synchronous worker replacement, and the matrix job has a
45-minute bound, so a hard worker loss fails promptly instead of entering a
replacement tail.

Local evidence after the fixes: the deterministic cleanup/CLI/workflow slice
passed; ten four-worker resilient repetitions passed; ten serial repetitions
of both streaming proofs passed; the full root suite passed serially with 18
expected skips; the dedicated serial coverage command passed; and Ruff
check/format, mypy, DOM-15 fixtures, plan-context, and `git diff --check`
passed. The deciding residual is the next Windows CI run. If a serial streaming
proof still reaches 360 seconds, capture that dedicated step's stacks and a
Windows native dump before changing the workload or timeout. Separately,
phase-lock currently spends its full 20-second budget on permanent Windows
invalid-filename `EINVAL`; that slow rejection is not needed to support an
unrepresentable filename and is a follow-up retry-classification question, not
part of this CI repair.

Independent completed-work review: **PASS after one P2 documentation fix.**
The reviewer found no behavioral or CI-policy blocker and verified the cleanup
zero-delete gate, retained checkpoint assertions, serial Windows coverage
append, xdist fail-closed policy, and local gates. Its only finding was that the
Status Index still described the `a38e6a9` implementation as uncommitted; the
index remains `active` but now records that committed baseline and the unrun
K0/K release gates accurately.

### Second-run CI remediation (2026-08-07)

The exact-SHA verification run for the first remediation, GitHub Actions run
[`31190183571`](https://github.com/VanL/simplebroker/actions/runs/31190183571),
had three failed Windows jobs: Python 3.12, 3.13, and 3.14. All non-Windows,
PostgreSQL, and Redis jobs passed. Windows 3.11 also passed, including the new
dedicated serial streaming phase.

The failures divide into one deterministic portability assertion and two test
coordination defects; current evidence does not show a new production-code
failure:

- Python 3.13/3.14 reached the cleanup embedded-NUL rejection through `lstat()`
  rather than `Path.resolve()`. Both routes raise the promised clean,
  pre-deletion `DatabaseError`, and the separate observation-failure test pins
  zero mutation. The test now accepts either truthful operation name instead
  of depending on a Python-minor implementation detail.
- Python 3.12 timed out four isolated SQLite CLI commands on four xdist workers
  at the same point in the run. Moving the prior streaming/resilience tests did
  not remove the shared-load cause; it moved the collision to one large
  broadcast and three vacuum tests. Windows full-suite concurrency is now fixed
  at two workers instead of host-derived `auto`. Vacuum tests seed state through
  `BrokerDB` and retain subprocesses only for the CLI vacuum behavior under
  test. The 10 MiB broadcast assertion keeps its real stdin/CLI path but applies
  the repository's CI timeout scaling to its explicit 20-second safety valve.
- Python 3.14's corrupt-to-readable coverage transition used a 50 ms writer
  sleep, a one-second reader deadline, and a two-second Windows replacement
  retry. The reader could exhaust its aggregate deadline before the writer
  published the valid generation. All three coverage writer-transition tests
  now use observation events instead of sleeps, and give the aggregate reader
  deadline room to cover the writer's bounded retry.

Local evidence after the second-pass fixes: the five-file cleanup, workflow,
coverage-script, vacuum, and safety slice passed under two-worker xdist (one
expected non-Windows open-handle skip); the four-test mixed broadcast/vacuum
set passed ten consecutive two-worker runs (40 assertions); and the exact
coverage corrupt-to-readable, empty-to-readable, and readable-snapshot-change
transitions each passed 25 consecutive coverage-enabled serial runs (75
assertions). The Windows workflow rerun remains the platform confirmation gate.

Independent second-pass CI review: **PASS with no findings.** The reviewer
checked the three job classifications against run `31190183571`, both Windows
full-suite worker-budget seams, the cleanup zero-delete control, retained CLI
vacuum/broadcast assertions, and all three event-driven coverage transitions.
Focused workflow, cleanup, vacuum, safety, coverage-transition, Ruff, YAML, and
plan/document gates passed. The reviewer agreed that the next Windows Actions
run is the remaining platform confirmation.

### First release-gate attempt and watcher synchronization (2026-08-07)

`bin/release.py all` ran from committed SHA `fb4b4a25`, passed its complete
local root, benchmark, PostgreSQL, Redis, examples, lint, type, lock, build,
and clean-wheel-install gates, then pushed `main` and waited for exact-SHA CI.
The PostgreSQL and Redis workflows passed. All four Windows jobs in Test passed,
including the prior 3.12 timeout set, 3.13 cleanup assertion, and 3.14 coverage
transition failures. This confirms the second Windows remediation on the
owning platform.

The Test workflow failed only on Ubuntu Python 3.14 at
`TestQueueWatcher.test_graceful_shutdown_sigint`: the child did not exit after
the requested SIGINT, and cleanup escalated to SIGKILL. The test's ready file
was published before `QueueWatcher.run_forever()` installed its signal handlers
or entered the run lifecycle, so the parent could signal a bootstrap handoff
instead of the active-watcher behavior named by the test. The helper now uses a
test-only `QueueWatcher` subclass to publish readiness after `run_forever()` has
installed the real signal handlers and completed polling-strategy startup. This
also preserves the helper's executable empty-queue transitions. The parent uses
the managed process-group interrupt seam and includes child stdout/stderr in any
escalation failure. The exact test passed 25 consecutive four-worker local
runs; the helper's six-case transition table and full watcher module passed;
Ruff, format, mypy, and diff checks passed.

Independent review initially **blocked** the handler-based readiness draft: the
helper's `ready`, `retry`, and `interrupt` transition probes intentionally use
an empty queue, so a data-callback-only marker could never fire. The strategy-
startup hook above resolves that deterministic regression while retaining the
stronger active-lifecycle proof; the previously failing three cases and the
full six-case table now pass.

Independent re-review: **PASS.** It confirmed that signal handlers and the real
polling strategy precede readiness, the immediate post-readiness stop path
reaches normal cleanup, empty-queue transitions remain live, and POSIX
process-group delivery is valid while Windows stays an explicit termination
seam rather than a graceful-SIGINT assertion.

The release helper observed the failed Test workflow and refused to push any
tag. Local and remote checks confirmed that `simplebroker_pg/v3.5.1`,
`simplebroker_redis/v3.5.1`, and `v6.0.2` do not exist. No package or GitHub
Release was published. A new committed SHA and full `bin/release.py all` pass
remain required; the failed workflow must not be rerun as release evidence for
the changed helper.

### Second release-gate attempt and backend marker scope (2026-08-07)

The next `bin/release.py all` pass ran from committed SHA `ab21b689`. Its full
local gate set passed again, it pushed `main`, and fresh exact-SHA workflows
started. The release helper stopped before tags when Redis Python 3.13 failed
the same watcher SIGINT test. The improved diagnostics proved that this was no
longer a startup-readiness failure: the child printed both
`READY_FOR_SIGNALS` and `Received: test_message` before shutdown escalation.

The test should not have been in that workflow. `tests/test_watcher.py` is
module-marked `shared`, while this method is function-marked `sqlite_only`.
Pytest markers are additive, so the wrappers' `-m shared` expression selected
the method despite the narrower function marker and ran its file-backed helper
with `BROKER_TEST_BACKEND=redis`. Collection found 22 dual-marked nodes across
six shared modules. PostgreSQL and Redis wrappers now select
`shared and not sqlite_only`; `--fast` additionally excludes benchmarks. The
normal PostgreSQL command and both fast wrapper commands were pinned red first,
then passed with the corrected expressions. Real `--fast` wrappers passed:
PostgreSQL ran 1,117 shared and 175 extension tests with eight expected skips;
Redis ran 1,109 shared and 246 extension tests with twelve expected skips.

The Redis workflow failure caused the helper to refuse publication immediately.
The same SHA's Ubuntu Python 3.14 core job passed, confirming the watcher
synchronization fix on the originally failing platform, and its PostgreSQL
workflow passed. No release tag was pushed. A third committed SHA and complete
release pass are required; the older exact-SHA workflows are not release
evidence for this wrapper change.

Independent marker-scope review: **PASS.** It confirmed that wrappers, not the
generic collection hook, own backend-valid selection; user `-m` filters remain
AND-composed and cannot re-admit SQLite-only tests; explicit extension routing
is unchanged; and the normal/fast help and command tests cover the expression
variants.

## Out of Scope

- The five deferred units (C, D, F, G, H) — register above with
  reopen conditions; their findings stay pinned in the maintainer
  ledger.
- A marker/xattr, connection-lifetime cleanup lock, quiescence detection,
  checkpoint-before-delete protocol, or any defined storage outcome for
  concurrent SQLite cleanup. Revision 5 deliberately specifies destructive
  deletion instead.
- Coverage-pipeline simplification; version compatibility matrix; broader
  xdist redesign; alternatives comparison.
- Message-ID non-canonical-spelling acceptance ([SB-ID-4]).
- Any poisoning-machinery, redis, pg-extension, watcher, or
  config-bootstrap code change.
- Any new config key; any Lua change.
- Any automatic cross-OS-user SQLite permission provisioning or repair. A future
  design must cover the full database/companion namespace and directory policy.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| T-A coverage node "bounded Weft retained-form smoke" | Weft-side smoke matrix over retained integer/ISO bound forms lands with Unit A | Deferred — not implemented in this train | Weft pins simplebroker from PyPI; 6.0.2 is unpublished, so a weft-side matrix cannot execute until the synchronized publish. Implementing agent raised it; owner approved the deferral 2026-08-07. Runs as a post-publish verification alongside K's clean-install checks. | None — plan-internal deliverable, no spec text affected |

(One owner-approved deferral row above; otherwise empty through the verified implementation.)

## Fresh-Eyes Review (revision 2 authoring pass; historical)

Checked against writing-plans §10 and the hardening checklist: the
reduced train has no dependency on any deferred unit (verified task
by task); both spec deltas carry exact text and land atomically with
their units; every revision-1 claim the review proved false is
corrected in place (surface names, reopen premise, residue model,
release order, rollback claim) rather than papered; the recovery
table named every assumed immutable prefix. Round 2 later rejected that
publication-prefix model while retaining the current async mechanism.
Deliberately surfaced for round
2: whether T-B's runner-level normalization should also cover modes
on databases *opened* by direct runner use without `BrokerDB` (the
plan says yes — the guarantee is runner-owned); and whether T-E's
concurrent-handle test can be made deterministic without a fixed
sleep (deadline-poll on the second handle's next operation).

## Independent Review Findings (2026-08-06)

Review baseline: `3cb6e09` (working tree review; this plan and its
Status Index row are uncommitted). The primary reviewer checked the
plan against the winning product specs, program theory, implementation
maps, named code and tests, the release driver, Weft's downstream use,
and live reproductions. Three focused independent passes covered the
CLI contract, SQLite/lifecycle behavior, and Redis/release behavior. A
time-boxed outside Codex pass independently reproduced F1, F4, F8, and
F10 before its process was stopped. No implementation files were
changed by the review.

### Verdict

**BLOCKED.** The plan fails both implementation-readiness questions:

1. An engineer cannot implement it confidently because several named
   surfaces, tests, and release steps do not exist or behave differently
   from the plan.
2. Literal implementation would degrade the system through at least
   two deadlocks, an underspecified destructive lifecycle, and an
   irreversible partial-publish risk.

The plan must remain `draft`. Each F1–F10 item needs an explicit
disposition, followed by another independent review, before any code
slice starts.

### Findings

| ID | Sev. / confidence | Evidence | Finding | Required disposition |
|----|-------------------|----------|---------|----------------------|
| F1 | P1 / 10 | `simplebroker/_timestamp.py:355,411,456,544`; `tests/test_after_flag.py:29-40`; `simplebroker/sbqueue.py:390-456`; downstream Weft `../weft/weft/commands/queue.py:936` | T-A treats all numeric-looking forms as one grammar, but suffix parsing precedes the proposed gate and accepts fractional seconds. The CLI flags are `--after`/`--before`, not `--since`; library `after_timestamp` accepts an integer and does not use the string parser. | Round 1 called for three grammars and retention of the then-intentional fractional behavior. The round-2 owner amendment supersedes the retention part: all three grammars are now integral-only. |
| F2 | P1 / 10 | `simplebroker/db.py:3589-3618`; `simplebroker/_runner.py:333-343`; `simplebroker/_phaselock.py:933-951`; `tests/test_portability.py:105-106` | T-B's reopen premise is false. Under umask `022`, first-session and reopened `-wal`/`-shm` files remain `0644`; direct public `SQLiteRunner` construction also creates a `0644` main file. Copying the phaselock create pattern without an exact race contract can introduce TOCTOU failures. | Define the ownership boundary (`BrokerDB` only versus stable `SQLiteRunner` API), the legacy-sidecar policy, and the exact `O_CREAT|O_EXCL|O_WRONLY` race behavior. Test in a subprocess with umask `022`, test reopen, and retain the existing eight-process setup race gate. Narrow the security claim if direct runner construction remains outside it. |
| F3 | P1 / 10 | `simplebroker/__init__.py:7`; `simplebroker/_broker_session.py:19`; `simplebroker/cli.py:40,1519`; `simplebroker/_constants.py:651`; downstream Weft `../weft/weft/_constants.py:2255-2274` | T-C's CLI catch is unreachable in the named scope: eager package/module configuration loads before `main()`, so malformed env input raises during import. Weft also calls `resolve_config(overrides)`, which the proposed named-key wrapper does not classify. | Either defer T-C or expand it into an explicit bootstrap/eager-config redesign. Define diagnostics for environment values versus programmatic overrides and verify `broker`, `python -m simplebroker`, library import, and Weft's override path. |
| F4 | P1 / 10 | `docs/specs/10-cli.md:36-50` [SB-CLI-2]; `simplebroker/commands.py:798-819`; `simplebroker/cli.py:868-879` | T-D creates unconditional human stderr commentary without a `--quiet` matrix, contradicting [SB-CLI-2]. Its premise that `--status` already reports provenance is false: status emits metrics. The notice behavior is a public CLI contract change, not a code-debt clearance. | Defer T-D from this release unless the owner explicitly chooses the UX change. If retained, add a canonical spec delta and a command/quiet/JSON/source-precedence matrix, including an explicit decision for `--status`. |
| F5 | P1 / 9 | `simplebroker/_backends/sqlite/plugin.py:104-120`; `simplebroker/_backends/sqlite/maintenance.py:148-174`; `simplebroker/_phaselock.py:865-874` | T-E is a destructive persistence change, yet the plan assigns it vaguely to CLI or runner code and asserts that only `.lock` remains. `.status` and `.vacuum.lock` are legitimate residues. It does not define ordering, validation, partial failure, idempotence, concurrent SQLite handles, or the absent-main/present-sidecar case. | Keep cleanup ownership in the SQLite backend plugin. Define the full owned-sidecar allowlist and safe state machine, including validation-before-delete, absent-main behavior, partial failure, concurrency, and idempotence. Add a canonical persistence/ops spec delta and adversarial filesystem tests. |
| F6 | P1 / 10 | `simplebroker/db.py:3017-3049,3438-3486`; `extensions/simplebroker_pg/simplebroker_pg/plugin.py:706-739`; `simplebroker/_backend_plugins.py:32-45` | T-F expands the optional backend-hook invocation contract from add/rename to remove and creates a PostgreSQL lock-order cycle: add/remove would take alias advisory lock then the meta row, while rename takes the meta row then alias advisory lock. A call-recording unit cannot detect the deadlock. The plan's "SQLite no hook/byte-identical" premise is also false because SQLite supplies a no-op hook. | Defer T-F unless rewritten as a full PostgreSQL lock-order and backend-API compatibility slice. Choose one global lock order, decide whether `BACKEND_API_VERSION` changes, update the hook contract, and add a forceable two-connection deadlock test with bounded completion. |
| F7 | P1 / 10 | `simplebroker/watcher.py:1003,1076-1105`; `docs/program-theory.md`; `docs/implementation/07-complexity-and-state-machine-map.md:230` | T-G's preferred finalizer cannot run for a live watcher: `Thread(target=self.run_forever)` strongly retains `self`. Extracting strong resource references does not fix reachability and risks bypassing owner-thread and shutdown ordering. This is explicitly an ownership/lifecycle question, so the plan's `Theory: N/A` claim is false. | Drop the impossible GC promise and make explicit close/context management the truthful contract, or create a separate watcher ownership redesign with a non-bound thread target, lifecycle transitions, cross-thread rules, and proof that cleanup can become reachable. |
| F8 | P1 / 10 | `extensions/simplebroker_redis/simplebroker_redis/core.py:72-90,274-307,1498-1678`; `docs/implementation/08-message-identity-and-write-visibility.md:71-131`; `docs/implementation/07-complexity-and-state-machine-map.md:234-235` | T-H's adjacent lock change self-deadlocks patterned broadcast because `broadcast()` delegates to `insert_messages()` and `_SharedWriteLock` uses `threading.Lock`. The task conflates ordinary write, exact-ID insert, atomic broadcast, and patterned broadcast even though exact duplicate insertion must remain immediate under [SB-ID-4]. It also omits existing first-conflict sleep, second-conflict resync, stale-fence refresh, and shared-budget semantics, and names nonexistent `evalsha` rather than `.eval`. | Defer T-H unless current contention evidence makes it a release blocker. Otherwise redesign it from the registered Redis state machines: use a private unlocked primitive or a justified reentrant boundary, preserve each result-code transition, retain exact-insert behavior, update implementation docs/tables, and test `.eval` with a fake clock and small budget plus real Valkey contention. |
| F9 | P1 / 10 | `pyproject.toml:103`; `extensions/simplebroker_pg/pyproject.toml:44`; `extensions/simplebroker_redis/pyproject.toml:43`; root and extension `test_*cross_thread_generator_probe.py` files | T-I cannot meet its own claim by setting one root-job environment variable. Root pytest only collects `tests/`; the PostgreSQL and Redis probes belong to separate extension suites and workflows. A job-level variable on the root matrix would also run more than one Linux instance. | Wire one explicit non-coverage Linux instance in each owning workflow: root, PostgreSQL, and Redis. Assert collection/execution in CI and record the prior closure gap. |
| F10 | P1 / 10 | `bin/release.py:374-378,1899-1915,2065-2107,2214-2335,2465-2467`; release-script tests | T-K and comprehension question 4 contradict the release machinery. Batch release orders PostgreSQL, Redis, then core and pushes all tags without waiting for publication between them; standalone core refuses unpublished extension baselines. PyPI publication precedes the final GitHub release and versions are immutable, so "a failed publish leaves PyPI at the old coherent state" is false. | Add a K0 release-mechanics decision before implementation. Reconcile bidirectional dependency floors with an actually executable tag/publication order, add publication waits and clean-environment package checks, and define recovery for every partial-publication prefix. Do not claim rollback after any PyPI publish. |

### Ratified judgments and scope recommendation

- Keep A, B, I, and K as release blockers, after revising them for F1,
  F2, F9, and F10. These address a real contract ambiguity, a real
  file-mode exposure, missing CI evidence, and the release one-way
  door.
- Keep E only after the backend owner, sidecar allowlist, destructive
  state machine, and winning spec delta are explicit. J should either
  depend on the final included unit set or move each release note into
  its owning task; it is not independently revertible as written.
- Defer D and G. D is a new noisy CLI policy, not pre-release hygiene.
  G's preferred mechanism is structurally impossible while the thread
  target retains the watcher.
- Defer H unless there is measured release-blocking contention. It is
  a registered state-machine redesign, not a small retry adjustment.
- Defer F unless the release accepts a full PostgreSQL lock-order and
  backend-plugin API slice. The one-line symmetry fix is unsafe.
- Defer C unless the release accepts a broader import/bootstrap
  redesign. A catch in `main()` cannot intercept eager import failure.

This yields a coherent reduced release train of revised A, B, E, I,
J, and K. C, D, F, G, and H should not remain in the atomic batch by
default.

### Existing seams to reuse without overstating them

- `TimestampGenerator.validate()` is the public string-parser choke
  point. Integer range filters in `Queue` are a separate contract.
- The phaselock exclusive-create code is a useful secure-create
  primitive, but it does not decide DB ownership or legacy-sidecar
  policy.
- SQLite cleanup already belongs to the backend plugin. Extend that
  lifecycle owner instead of adding CLI-only file deletion.
- The vendored retry helpers are usable mechanisms, but the registered
  Redis write/broadcast state machines remain the semantic owner.
- `bin/release.py` and its tests are the executable release authority.
  The plan must adapt to or deliberately revise that machinery.

### Coverage map required by the revision

```text
A  string grammar -> public TimestampGenerator.validate -> CLI --after/--before
                    \-> Weft parser compatibility
B  BrokerDB create -> main/WAL/SHM mode -> reopen + concurrent setup
                    \-> direct SQLiteRunner claim or documented exclusion
E  SQLite cleanup  -> owned-sidecar state machine -> race/partial failure tests
I  root workflow   -> root probe
   pg workflow     -> PostgreSQL probes
   redis workflow  -> Redis probes
K  tag order       -> workflow publish -> PyPI visibility wait -> clean installs
                    \-> recovery for each partial-publish prefix
```

### Worktree parallelization strategy

No implementation lane may launch until F1–F10 are dispositioned and
K0 establishes an executable release mechanism. After re-review, the
likely lanes are:

| Lane | Modules | Depends on |
|------|---------|------------|
| Timestamp | timestamp parser, CLI-selection tests, CLI/API specs | round-2 owner amendment recorded |
| SQLite | creation and cleanup, persistence/ops specs | round-2 F2/F5 remediation and another review |
| CI probes | root and extension workflows, workflow-policy test | amended F9 task |
| Integration | changelogs, guides, plan/status index, release checklist text | final included set; no release-driver change |

Timestamp and CI-probe implementation may proceed in separate worktrees only
after the overall plan gate is lifted. Do not launch the SQLite lane until both
round-2 SQLite findings are dispositioned and re-reviewed. Merge shared
`CHANGELOG.md` edits through the Integration lane. Original units C, D, F, G,
and H remain excluded unless explicitly restored; F and H would need separate
worktrees and independent post-slice reviews because they touch registered
concurrency state machines.

## Finding Dispositions (Revision 2)

| ID | Disposition |
|----|-------------|
| F1 | **Accepted in revision 2, then superseded by the round-2 owner amendment.** The three-grammar partition remains, but every fractional-second spelling is now rejected. Unit A must update existing acceptance tests, CLI help, README, [SB-CLI-5], and the public validator contract; Weft has no fractional dependency. |
| F2 | **Accepted in revision 2; BLOCKED again in round 2.** Unit B misses the earlier runtime-setup connection, `:memory:`, symlink-resolved sidecars, chmod-failure policy, and a winning public API/ops contract. Its cited eight-process gate is also wrong. See R2-F3/R2-F4. |
| F3 | **Deferred (owner-ratified).** A `main()` catch cannot intercept eager import-time config loading, and Weft's `resolve_config(overrides)` needs its own diagnostics. Register row names the bootstrap-redesign reopen condition. |
| F4 | **Deferred (owner-ratified).** Unconditional stderr commentary is a CLI-contract change conflicting with [SB-CLI-2]; the `--status` premise was false. Reopens only with an owner UX decision plus a full quiet/JSON/status/source-precedence spec delta. |
| F5 | **Accepted in revision 2; BLOCKED again in round 2.** Plugin ownership is right, but manual WAL/SHM unlink under live handles is unsafe, the plugin has no owned handle to close, and absent-main sidecar deletion contradicts validation-before-delete. See R2-F1/R2-F2. |
| F6 | **Deferred (owner-ratified).** The symmetry fix creates a pg lock-order cycle (add/remove: advisory→meta vs rename: meta→advisory); requires a global lock-order slice with a two-connection bounded-completion deadlock test and a backend-API decision. Register row carries the reopen condition. |
| F7 | **Deferred (owner-ratified).** GC-time cleanup is structurally impossible while `Thread(target=self.run_forever)` retains the watcher; the truthful contract is explicit close/context management; theory-bearing per [REV-THEORY-005], so revision 1's `Theory: N/A` is withdrawn (header corrected). Reopens as a dedicated lifecycle redesign or an honest docs-only fix. |
| F8 | **Deferred (owner-ratified; supersedes the earlier in-session owner direction to apply `_retry` directly).** The adjacent lock change self-deadlocks patterned broadcast (`broadcast()` → `insert_messages()` under non-reentrant `_SharedWriteLock`); the unit conflated four write paths; exact-insert immediacy ([SB-ID-4]) and the existing conflict/resync/budget semantics were omitted; the real seam is `.eval`. Reopens with contention evidence or a registered-state-machine redesign. |
| F9 | **Accepted and round-2 hardened.** The three exact workflows, one matrix condition each, explicit opt-in assertion, structural policy test, and per-wrapper container lifecycle are now named. Exit 5 is only the collection guard; the environment assertion guards execution. |
| F10 | **Accepted in revision 2; corrected and owner-ratified in round 2.** `bin/release.py` pushes tags pg → redis → core, while the workflows publish independently. The working asynchronous mechanism stays unchanged. K0 now separates tag order from completion order and uses per-workflow reruns rather than a false ordered-prefix recovery model. |

## Implementation Tasks

Synthesized from this review's findings. These are plan-remediation
tasks, not authorization to change product code.

- [x] **T1** — Timestamp contract — three grammars specified; real
  public test surfaces named (Unit A; Delta 1). **Superseded in round 2:**
  the owner chose integral seconds only; the amended T-A above is the current
  task and must be reviewed as part of the next revision.
- [ ] **T2** — SQLite security boundary — resolve R3-F5–R3-F8 and the seven
  missing coverage branches before implementation: exact versus ceiling mode
  under restrictive umask;
  lazy first-connect behavior; one frozen path identity for helper, both
  connects, and phase locks; validation/type/no-follow gates before existing
  target or sidecar mutation; the compatibility policy for intentionally
  shared databases; a real barrier-synchronized creator race; and an explicit
  non-POSIX bypass or contract.
- [x] **T3** — Configuration bootstrap — **deferred** with reopen
  condition (register row C).
- [x] **T4** — Provenance UX — **deferred** with reopen condition
  (register row D).
- [x] **T5** — Cleanup lifecycle plan remediation — **restored by owner in
  revision 5.** Delta 2 defines full deletion of the known SQLite/SimpleBroker
  filesystem namespace, including phaselock, vacuum-lock, and status-temp
  residue. It deliberately adds no quiescence protocol: concurrent storage and
  client outcomes are undefined, while deletion attempts and CLI reporting are
  defined.
- [x] **T6** — PostgreSQL alias locking — **deferred** with reopen
  condition (register row F).
- [x] **T7** — Watcher ownership — **deferred**; impossible GC
  promise dropped; theory header corrected (register row G).
- [x] **T8** — Redis conflict state machine — **deferred** with
  reopen condition; supersession of earlier owner direction recorded
  (register row H).
- [x] **T9** — CI probe ownership — three owning workflows (Unit I).
- [x] **T10** — Release mechanics — existing asynchronous mechanism
  owner-ratified; K0 now separates tag order from completion order and uses
  per-workflow recovery. No release-driver change.
- [x] **T11** — Plan integration — J rebuilt from the final set;
  `Theory: N/A` replaced with concrete records; this disposition
  table added; revision-5 Unit E re-review completed PASS below.

### Review Log

| Date | Reviewer | Scope | Result | Notes |
|------|----------|-------|--------|-------|
| 2026-08-06 | Codex primary + CLI, SQLite/lifecycle, and Redis/release independent passes; time-boxed outside Codex corroboration | Full plan, named code/tests/specs, Weft seam, live CLI/filesystem probes, release driver | BLOCKED | F1–F10 open; literal execution risks PostgreSQL and Redis deadlocks plus irreversible partial publication. |
| 2026-08-06 | Codex primary + focused CLI, SQLite/lifecycle, and Redis/release passes; time-boxed outside Codex pass | Revision 2 dispositions, owner amendments, named code/spec/test/workflow seams | BLOCKED | Timestamp amendment and CI task repaired; current release mechanics retained by owner. Unit B still lacks a complete creation/security contract; Unit E's live-WAL deletion is unsafe and internally contradictory. |
| 2026-08-06 | Codex primary + focused Unit B, Unit E, and holistic independent passes; Codex CLI outside voice | Revision 3 B/E rebuild, exact real-SQLite protocol probes, file-mode/umask/path-identity review | BLOCKED | Unit E's prescribed checkpoint fails inside the transaction and open-file unlink is corruption-prone; Unit B retains seven contract and test gaps. |
| 2026-08-06 | Product owner | Round-3 Unit E disposition | ACCEPTED SCOPE REDUCTION | Full/concurrent cleanup deferred; no `[SB-OPS-7]`, marker, xattr, or lifecycle lock in 6.0.2; J documents current limitations. |
| 2026-08-06 | Product owner + primary experimental re-review | Revision-5 Unit E disposition; 540 macOS/glibc/musl process cases; SQLite upstream boundary; interface contract | ACCEPTED DESTRUCTIVE CONTRACT | Revision-4 deferral superseded. Cleanup owns the full known target namespace and does not preserve overlapping work. Exact concurrent SQLite outcomes remain undefined; CLI attempt/diagnostic semantics remain defined. |
| 2026-08-06 | Independent SQLite lifecycle reviewer | Latest Revision-5 Unit E allowlist, best-effort state machine, interface/docs/test ownership, and platform branches | PASS (UNIT E) | Two focused correction audits closed observation/boolean, coordination-overlap, diagnostics, grammar, enumeration, and coverage gaps. Existing 14-test slice and plan-context gate passed. |

### Runbook feedback

The repository's independent-review and hardening guidance correctly
forced source verification of named seams and one-way doors. A useful
future addition to `writing-plans.md` is an explicit rule: when a plan
names an existing release or CI driver, compare its proposed order and
test collection roots to executable code before accepting prose claims.

### Completion summary (round 1)

- Step 0, scope challenge: scope reduction recommended.
- Architecture review: seven blocking ownership, lifecycle, lock-order,
  state-machine, bootstrap, and release-mechanics issues.
- Code/contract review: three blocking grammar, CLI, and destructive
  contract issues.
- Test review: coverage map produced; nine material test-surface or
  collection gaps identified across F1–F10.
- Performance review: no standalone tuning approved. The Redis retry
  proposal lacks release-blocking contention evidence and introduces a
  deadlock before any throughput benefit can be evaluated.
- Failure modes: four critical degradation paths flagged.
- Unresolved decisions: ten (all dispositioned in revision 2).

## Independent Re-review Findings (Round 2, 2026-08-06)

### What exists today

- Timestamp fractions are accepted by three distinct parser paths and pinned by
  existing public tests. The round-2 owner amendment intentionally removes
  that behavior and names integer `ms`, integer `ns`, and native IDs as the
  finer-grain paths.
- SQLite can first connect through backend runtime setup before
  `SQLiteRunner.get_connection()`. `SQLiteRunner(":memory:")` is a stable,
  exercised public target. Existing chmod failure warns and continues.
- SQLite cleanup is plugin-owned, but the plugin is stateless and owns no live
  database connection to close or checkpoint.
- The three finalization probes are opt-in. If the variable is absent, pytest
  collects and skips them with exit 0; exit 5 alone cannot prove execution.
- The release driver pushes three tags from one green SHA and the independent
  workflows publish asynchronously. The owner has ratified that working
  mechanism.

### Round-2 verdict

**BLOCKED.** Units A, I, J, and K are plan-ready after the owner amendments and
round-2 corrections. Deferred C, D, F, G, and H remain cleanly severed. Units
B and E are not safe or complete enough to implement.

### Actionable findings

| ID | Severity / confidence | Evidence | Finding | Required disposition |
|----|-----------------------|----------|---------|----------------------|
| R2-F1 | P1 / 10 | `tests/test_validation_lock_safety.py:1–17`; `simplebroker/_backends/sqlite/plugin.py:104–120`; `simplebroker/_backends/sqlite/validation.py:50–97`; official SQLite [WAL lifecycle](https://sqlite.org/walformat.html#file_lifecycles) | Unit E's sidecars-first algorithm manually separates a live database from its WAL. Real probes produced `no such table`, `disk I/O error`, invisible committed rows, and a racing opener that recreated a WAL after the cleanup step. The plan's “plugin's own handle” does not exist. SQLite states that WAL deletion is normally performed by the last connection while holding the exclusive lock. | Do not implement raw WAL/SHM unlink under live handles. Either defer sidecar deletion or specify an SQLite-mediated quiescence/exclusive-lock protocol and prove old holder, racing new opener, post-close durable rows, and residue. |
| R2-F2 | P1 / 10 | Plan Context E and proposed [SB-OPS-7]; `simplebroker/_backends/sqlite/validation.py:50–97`; `docs/lessons.md` 2026-08-04 cleanup-authority lesson | Unit E says every deletion follows main-file validation, then deletes same-basename sidecars when the main file is absent. A basename is not ownership proof. The branch also is not needed to recover a partial state created by its own sidecars-first order, because that order retains the main file until last. | Remove absent-main sidecar deletion unless a separate durable ownership proof is designed. Define success/return semantics and partial-failure ordering only after the concurrency model is safe. |
| R2-F3 | P1 / 10 | `simplebroker/db.py:997–1003`; `simplebroker/_backends/sqlite/runtime.py:92–119`; `simplebroker/_runner.py:333–343`; `tests/test_backend_plugin_resolution.py:89–100` | Unit B puts secure creation only in `get_connection()`, but runtime setup can connect first. A naive file pre-create also turns `SQLiteRunner(":memory:")` into a literal file. | Put filesystem-only secure creation behind one shared helper used by both first-connect paths. Explicitly preserve `:memory:` and decide URI/empty targets. Add the stable in-memory runner regression and cite the real eight-process gate at `tests/test_runner_error_handling.py:1112–1139`. |
| R2-F4 | P1 / 9 | `simplebroker/db.py:3608–3625`; `simplebroker/_runner.py:211`; `docs/guides/configuration.md:489–497`; `docs/specs/16-python-library-api.md:223–236` | Unit B has no chmod-failure policy. Best-effort behavior makes an unconditional 0600 guarantee false; fatal behavior can break adopted or intentionally shared databases. Raw symlink paths also miss WAL/SHM created beside the resolved target. The task names README but not the current security guide or a winning public API/ops contract. | Choose fatal versus best-effort normalization and state the compatibility cost. Define symlink canonicalization, add a real symlink test, update the actual security guide, and add the public runner contract delta. |

### Ratified judgments

- **Unit A passes plan review after amendment.** All fractional-second forms,
  including ISO fractional components, are rejected. The amended task names
  the public validator, CLI, README, winning CLI/API contracts, existing tests,
  actionable diagnostic, and Weft's bounded retained-form smoke matrix.
- **Unit I passes plan review after correction.** The exact three workflows,
  single matrix instances, explicit opt-in assertion, structural workflow
  policy test, and wrapper-owned container lifecycles are named.
- **K0/K are not blocked by the asynchronous release mechanism.** The owner is
  correct that no concrete product defect was shown. The plan now describes
  the existing mechanism accurately and names per-workflow recovery without
  changing the driver.
- **J and deferred C/D/F/G/H pass severance review.** Shared `CHANGELOG.md`
  integration remains a merge coordination point, not a semantic blocker.

### Agent-facing interface review

| Principle | Result |
|-----------|--------|
| 1. Context is scarce | Pass; no new routine output. |
| 2. Progressive disclosure | Pass after A adds help, README, spec, and API ownership. |
| 3. Self-explanatory names | Pass after worktree lanes were renamed by purpose. |
| 4. One identity per thing | Pass; explicit unit suffixes remain distinct representations. |
| 5. Derive what is derivable | Not applicable. |
| 6. No hidden setup | Pass for A/I/K; B/E remain blocked on hidden lifecycle state. |
| 7. Teach, do not merely reject | Deliberate owner-ratified departure for fractions; the diagnostic teaches integer `ms`/`ns` and native IDs. |
| 8. Every message carries an action | Pass in amended A; B/E error and recovery semantics remain unresolved. |
| 9. Atomicity and recovery | Blocked for Unit E; multi-file destructive cleanup lacks a safe atomic boundary. |
| 10. Trust boundaries are explicit | Blocked for absent-main sidecars; pathname similarity is not authority. |
| 11. Wire the mental model | Pass for integral timestamp units and current async release wording. |

### Required coverage map

```text
A  three integral grammars -> public validate -> CLI --after/--before
   -> help/README/spec/API -> bounded Weft retained-form smoke

B  filesystem target? -> shared secure-create helper
   -> runtime setup + get_connection
   -> :memory:/URI/symlink/chmod policy -> umask + 8-process tests

E  validated owned target -> exclusive/quiescent SQLite state
   -> checkpoint/close mediated by SQLite -> delete -> racing opener proof
   (no ownership proof => no absent-main sidecar deletion)

I  root workflow  -> opt-in assertion -> root probe
   pg workflow    -> opt-in assertion -> wrapper/container -> pg probes
   redis workflow -> opt-in assertion -> wrapper/container -> redis probe
   all three      -> structural workflow-policy test
```

### Critical failure modes

1. Deleting WAL/SHM beneath a live connection can hide committed data from a
   new connection or make the database unreadable.
2. A new opener can recreate a sidecar between unlink steps, defeating the
   cleanup postcondition while the main file is then removed.
3. Pre-creating `:memory:` creates the wrong persistence model.
4. A fatal chmod policy can break accepted shared/adopted databases; a
   best-effort policy can falsify the security promise.

### Parallelization after remediation

Timestamp and CI-probe implementation are independent. SQLite creation and
cleanup share target-path, connection, permission, and lifecycle policy, so
they should be revised together and independently re-reviewed before either
implementation starts. Integration owns shared CHANGELOG edits. The release
checklist remains an execution step, not an implementation lane.

### Review artifacts

- Test plan:
  `/Users/van/.gstack/projects/VanL-simplebroker/van-main-eng-review-test-plan-20260806-145926.md`
- Task JSONL:
  `/Users/van/.gstack/projects/VanL-simplebroker/tasks-eng-review-20260806-145926.jsonl`
- Outside voice: one read-only Codex CLI pass independently reproduced the
  fractional-contract drift, unsafe live-WAL cleanup, and missing CI
  structural gate before its time box ended. It made no edits.

### Not in scope

- Product-code implementation, commits, tags, workflow reruns, or publication.
- A release-driver redesign or serialized publication.
- Deferred units C, D, F, G, and H.
- Coalescing maintenance despite the separately reported threshold.

### Round-2 runbook feedback

The hardening and interface runbooks exposed the destructive-lifecycle and
teaching gaps. The prior recommendation to compare plan order with executable
drivers remains useful. One added review prompt would help: for SQLite WAL
cleanup, require proof that deletion is performed by SQLite while it owns the
exclusive lifecycle lock, not by pathname operations that bypass SQLite's lock
protocol.

## Finding Dispositions (Revision 3)

| ID | Disposition |
|----|-------------|
| R2-F1 | **Accepted — quiescence protocol chosen over deferral.** Cleanup contains zero raw sidecar unlinks; `-wal`/`-shm` deletion is performed by SQLite under its exclusive lifecycle lock (`BEGIN EXCLUSIVE` → checkpoint TRUNCATE → unlink main while holding the lock → close); busy at any acquisition point → refusal with zero deletions and proven post-refusal durability; the stateless plugin opens its own dedicated cleanup connection (the fictional "plugin's own handle" is corrected). All four demanded proofs (old holder, racing opener, post-close residue, refusal durability) are named tests in T-E. |
| R2-F2 | **Accepted — absent-main deletion removed entirely.** Basename is not ownership; absent-main is a successful no-op with orphan sidecars left in place and mentioned. The atomic boundary narrows to a single unlink of the validated main file, eliminating multi-file partial states; success/refusal/failure semantics defined per path (Context E). |
| R2-F3 | **Accepted — one shared secure-create helper on both first-connect paths** (runtime setup and `get_connection`), gated to plain filesystem targets: `:memory:` preserved byte-identically with a stable regression test; URI/empty targets documented as excluded from the creation guarantee; the real eight-process race gate (`tests/test_runner_error_handling.py:1112–1139`) cited and retained. |
| R2-F4 | **Accepted — split guarantee, symlink canonicalization, named public owners.** Creation-time 0600 unconditional (atomic in `os.open`); reopen normalization best-effort with the existing warning (today's documented behavior; zero compatibility cost for shared/adopted DBs); `os.path.realpath` before create/normalize so resolved-side wal/shm are covered, with a real symlink test; the contract lands in three named owners — configuration-guide security section, [SB-API] runner delta (Delta 3), README. |

## Independent Re-review Findings (Round 3, 2026-08-06)

### What already exists

- The two real SQLite first-connect seams are correctly identified:
  `_setup_connection_phase()` reaches `runtime.setup_connection_phase()`
  before ordinary `get_connection()`, while direct public runner use can reach
  `get_connection()` without runtime setup. A shared helper in `_runner.py` can
  serve both without reversing the runtime dependency.
- SQLite derives new WAL/SHM permissions from the main database. A live POSIX
  probe with a securely pre-created `0600` main file produced `0600` main,
  WAL, and SHM files under an ordinary umask. This supports Unit B's fresh-file
  mechanism, subject to the findings below.
- `validate_database()` checks the SimpleBroker magic through a short-lived
  read-only connection. It does not bind that validation to the later cleanup
  handle or pathname unlink, and its hand-built `file:` URI is unsafe for
  filesystem paths containing URI metacharacters.
- SQLite itself owns last-close WAL/SHM cleanup. That lifecycle is real, but it
  is driven by SQLite's main-file `SQLITE_LOCK_EXCLUSIVE`, not by SQL `BEGIN
  EXCLUSIVE` in WAL mode.
- The narrow current defect is reproducible without concurrency: a normal CLI
  write followed by successful `--cleanup` deleted the main database but left
  a 32 KiB `-shm`, a zero-byte `-wal`, and the intentionally persistent
  `.lock`. A later write and read at the same path succeeded. The demonstrated
  release issue is therefore incomplete filesystem cleanup and overbroad
  wording, not a failure of ordinary sequential queue reuse.

### Round-3 verdict

**BLOCKED.** Revision 3 cannot be implemented literally. Unit E has no valid
success path under its stated sequence and permits a race SQLite documents as
undefined and corruption-prone. Unit B's fresh-create direction is viable, but
its security and compatibility contracts are not yet complete enough for a
patch release.

### Architecture and code-quality findings

| ID | Severity / confidence | Evidence | Finding | Required disposition / suggested fix |
|----|-----------------------|----------|---------|--------------------------------------|
| R3-F1 | P1 / 10 | Plan Context E steps 3–4; real SQLite 3.50.4/3.51.0 probe; SQLite transaction and checkpoint docs | `BEGIN EXCLUSIVE` followed by `PRAGMA wal_checkpoint(TRUNCATE)` raises `OperationalError: database table is locked` even with no other connection. The literal uncontended path never reaches unlink. | Do not repair this by swapping two steps. Defer E, or replace it with a separately reviewed lifecycle design. |
| R3-F2 | P1 / 10 | Plan Context E steps 3 and required old-holder proof; SQLite `lang_transaction.html` §2.2; separate-process probes | In WAL mode, `BEGIN EXCLUSIVE` is the same as `BEGIN IMMEDIATE`: it excludes another writer, not idle holders or readers. Both an idle connection and an active reader coexisted with the claimed exclusive transaction. | Remove the false last-connection claim. A future enforceable SimpleBroker guarantee needs a shared-for-connection-lifetime / exclusive-for-cleanup application lock. |
| R3-F3 | P1 / 10 | Plan Context E steps 5–7 and racing-opener proof; SQLite `howtocorrupt.html` §2.5; no-race and racing probes | Unlinking the main file while its SQLite handle remains open is explicitly undefined and corruption-prone because old and replacement databases can share pathname-derived WAL/SHM files. Reordering checkpoint before the transaction still left WAL/SHM residue after close. | Recommended for this train: defer E and document the current offline/residue limitation. A future design must close SQLite before unlink and hold a separate lifecycle lock across close and deletion. |
| R3-F4 | P1 / 9 | Plan Context E platform-neutral Delta 2; POSIX-only step 5; `validation.py:50–97`; `cleanup_target()` boolean contract; CLI `_run_cleanup()` | E lacks a Windows success path, does not bind validation to the inode later unlinked, mishandles `?`/`#`/`%` paths through a hand-built URI, and has no backend result channel for busy/residue/orphan outcomes without bypassing `--quiet` and canonical stream rules. | A future plan must define platform branches, URI-safe and same-identity validation, backend API compatibility, a typed result/error channel, quiet behavior, exit codes, and backup/recovery semantics. |
| R3-F5 | P1 / 10 | Plan Unit B fresh-path and unconditional-0600 text; POSIX `open()` semantics; live umask `0777` probe | `os.open(..., 0o600)` is masked by the process umask. It produced mode `0000` under umask `0777`; exact `0600` is not unconditional. | Either promise only “no more permissive than 0600,” or `fchmod(fd, 0o600)` before close and make `fchmod` failure a creation failure. Test umasks `000`, `022`, `077`, and owner-bit-removing `0777`. |
| R3-F6 | P1 / 9 | Plan Unit B existing-path and symlink text; `runtime.py:101–107`; `_runner.py:335–343,863–875`; `_phaselock.py:551–558` | The helper would chmod an existing target before runtime validation, so it can mutate a non-database file or directory. Pathname chmod follows planted sidecar symlinks. Resolving only inside the helper also leaves SQLite connects and phase locks on the raw path, allowing distinct symlink aliases and retarget races. | Freeze one effective filesystem target for helper, both connects, and phase locks. Before changing existing files, validate type and authority; use descriptor-bound no-follow checks for main/sidecars, or narrow the reopen guarantee. Add invalid-file, directory, sidecar-symlink, two-alias, and retarget-boundary tests. |
| R3-F7 | P1 / 9 | Plan Unit B “compatibility cost: none”; current `BrokerDB` only chmods newly created main files | Successful normalization of an intentionally shared `0660` database silently removes group access. It does not “keep working with a warning.” Direct `SQLiteRunner` creation is also lazy, while the plan says construction creates the file. | Preserve lazy construction. Choose and disclose the shared-database policy. Recommended patch-release shape: guarantee secure fresh creation; on reopen do not silently rewrite the main file's sharing policy, and make any sidecar repair no more restrictive than the validated main policy. If forced 0600 is retained, record it as a breaking compatibility cost and add an owner decision. |
| R3-F8 | P2 / 10 | Plan T-B race controls and Windows control; existing eight-process schema-setup gate | The existing eight-process gate proves phaselock schema serialization, not the helper's real `O_EXCL` winner/loser race or final live sidecar modes. Running the helper on Windows also changes creation/race behavior even if chmod bits are ineffective. | Add a barrier-synchronized multi-process direct-runner race and stat sidecars while a connection remains live. If Windows neutrality is intended, bypass the entire helper on non-POSIX and test that precreation/normalization do not run. |
| R3-F9 | P2 / 10 | Plan header/source list before this review; proposed Delta 3 | Revision 3 introduced a third normative public delta but its class header, governing sources, and baseline still described only two. | Corrected in this review. Delta 3 remains proposed and blocked with Unit B. |
| R3-F10 | P2 / 10 | Plan Rollback and Rollout; B permission changes; E database deletion | “Nothing persistent” is false. B changes permissions persist across code rollback; E deletes data irreversibly. | Distinguish software rollback from operational recovery. B needs a recorded mode-restoration path; any future E needs explicit backup/restore guidance and a one-way-door confirmation. |
| R3-F11 | P1 / 10 | Uncommitted `docs/lessons.md` 2026-08-06 WAL/SHM entry | The new durable lesson repeats the rejected revision-3 fix: it says `BEGIN EXCLUSIVE` quiesces WAL cleanup and only the validated main file may be unlinked. Round-3 real probes and SQLite's own docs disprove both claims. Leaving it would promote unsafe negative knowledge into the always-read tier. | Before landing any related work, replace that lesson with the round-3 result: SQL `BEGIN EXCLUSIVE` is not SQLite's WAL last-connection lock; checkpointing inside it fails; never unlink an open SQLite database; enforceable full cleanup needs separate lifetime coordination. This review did not edit the concurrent user change. |

The outside Codex pass independently reproduced R3-F1–R3-F3 and R3-F5–R3-F8.
One outside-voice claim was rejected: `verify_magic=True` does check the
SimpleBroker magic. The remaining validation-to-unlink identity race is real.

### Agent-facing interface review

| Principle | Round-3 result |
|-----------|----------------|
| 3. Self-explanatory names | Unit B must say “first connection,” not runner construction. |
| 6. No hidden setup | Blocked: special-target and non-POSIX bypass rules are incomplete. |
| 8. Every message carries an action | Blocked: best-effort mode warnings do not state the unchanged security state or remediation. |
| 9. Atomicity and recovery | Blocked: checkpoint mutates before unlink; later close/residue failures follow an irreversible delete. |
| 10. Trust boundaries are explicit | Blocked: existing target and sidecar pathname similarity are not mutation authority. |
| 11. Wire the mental model | Blocked for E: SQL `BEGIN EXCLUSIVE` is not SQLite's last-connection lifecycle lock in WAL mode. |

### Required coverage map

```text
B  target classifier
   ├── :memory: / empty / file: special target -> unchanged behavior
   └── plain filesystem target
       ├── missing -> O_EXCL winner/losers -> umask/fchmod -> both connect seams
       └── existing -> validate type + authority before mutation
           ├── main policy + actionable warning
           ├── sidecar no-follow/regular-file checks
           └── one frozen identity -> connect + phase lock + symlink aliases

E  current revision-3 protocol
   ├── BEGIN EXCLUSIVE -> checkpoint -> SQLITE_LOCKED [PROVEN FAILURE]
   ├── idle/reader holder -> BEGIN succeeds [PROVEN THIRD OUTCOME]
   ├── unlink open main -> replacement shares sidecar names [CORRUPTION RISK]
   └── Windows open-handle unlink -> no success path

Future E lifecycle (proposal, not approved)
   shared lifetime lock on every SimpleBroker SQLite connection
   -> cleanup takes exclusive lifetime lock
   -> bind validation to exact target -> close SQLite
   -> delete closed target state -> release lock
   -> external SQLite opener limitation documented
```

Coverage status: Unit B has 7 material branches still missing from the task;
Unit E's planned success branch is disproved rather than merely untested.

### Performance review

No performance optimization is justified in this round. The added secure-open
checks are first-connection work. A future lifetime lock would affect every
SQLite connection and therefore requires contention and process-fanout
measurement in its own plan; that cost is another reason not to hide it inside
this pre-release train.

### Critical failure modes

1. Literal Unit E refuses every uncontended cleanup at checkpoint time.
2. An idle reader survives WAL `BEGIN EXCLUSIVE`, invalidating deletion safety.
3. Open-file unlink plus a new opener can split one pathname across two
   databases that share WAL/SHM names and corrupt either database.
4. Unit B can chmod an unvalidated or symlink-planted filesystem object.
5. Forced reopen normalization can silently revoke intended group access.
6. The concurrent uncommitted lessons change currently teaches the rejected
   cleanup protocol as durable guidance.

### Suggested revision-4 shape

- **Unit E: defer from 6.0.2 (recommended; accepted in revision 4).** Remove the
  cleanup delta and E's CHANGELOG claim. J owns the current cleanup limitation:
  cleanup is an offline operation; it does not prove that no other SQLite
  handle exists, and same-basename WAL/SHM residue may remain.
  Start a separate class-5 lifecycle plan only if full cleanup remains a
  product requirement.
- **Unit B: retain after one more focused rewrite.** Keep fresh secure creation,
  preserve lazy runner construction, define exact target classification and a
  trusted-directory/threat boundary, freeze one canonical path for connect and
  phase locking, validate existing objects before mutation, choose exact-mode
  versus mode-ceiling semantics, avoid silently breaking shared databases,
  bypass the entire helper on non-POSIX, and add the real race/adversarial
  tests named above.
- **Durable guidance:** correct the conflicting uncommitted WAL/SHM lesson
  before it lands. It must record the round-3 refusal, not the rejected
  revision-3 protocol.
- A, I, J, and K0/K remain plan-ready. C, D, F, G, and H remain cleanly
  deferred. Release execution stays blocked until B is repaired; E is now
  explicitly deferred.

### Parallelization after disposition

Timestamp and CI-probe lanes remain independent. Unit B stays isolated until
its focused rewrite passes review. Unit E has no implementation lane under the
owner-ratified deferral. Integration owns shared docs/CHANGELOG edits,
including the adopted cleanup limitation. K remains sequential after every
retained gate is green.

### Review artifacts

- Test plan:
  `/Users/van/.gstack/projects/VanL-simplebroker/van-main-eng-review-test-plan-20260806-154658.md`
- Task JSONL:
  `/Users/van/.gstack/projects/VanL-simplebroker/tasks-eng-review-20260806-154658.jsonl`
- Outside voice: read-only Codex CLI pass, 1 run, issues found; no edits.

### Not in scope

- Implementing product code, committing, tagging, rerunning workflows, or
  publishing packages.
- Designing the full future lifecycle-lock implementation inside this review.
- Reopening the owner-ratified timestamp, Weft, or asynchronous-release
  decisions without new contrary evidence.
- Deferred Units C, D, F, G, and H.
- Coalescing maintenance despite the separately known threshold.

### Round-3 completion summary

- Scope: no expansion approved; E recommended for removal from this train.
- Architecture: Unit E protocol disproved; Unit B trust and identity boundaries
  remain incomplete.
- Code quality: existing validation/result/phase-lock seams traced; no product
  code changed.
- Tests: exact real-SQLite failures reproduced; revised coverage map and QA
  artifact produced.
- Performance: no tuning approved; future connection-lifetime lock isolated to
  its own plan.
- Outside voice: Codex independently corroborated the blocking core; one false
  positive rejected against `validate_database()` magic checking.
- Parallelization: timestamp, CI, B, integration, then sequential release; E
  removed under the recommended disposition.

### Round-3 runbook feedback

Add a candidate secure-create prompt to future hardening guidance: distinguish
an exact mode from the umask-masked creation ceiling, and prove that the path
secured, the path opened, and the path locked are the same identity. This is
useful but not yet promoted from one review.

## Finding Dispositions (Revision 4)

| ID | Disposition |
|----|-------------|
| R3-F1–R3-F4 | **Accepted by scope reduction.** Unit E, the cleanup spec delta, and its Fixed CHANGELOG claim are removed from the release train. J states the current SQLite contract plainly: offline operation, stop all clients, main database file only, WAL/SHM residue possible, phaselock files outside cleanup, and no quiescence/concurrency guarantee. No marker, xattr, or lifetime lock is added. |
| R3-F5–R3-F8 | **Open and blocking Unit B only.** Revision 4 must choose exact-mode versus ceiling semantics, preserve lazy construction, define existing/shared-target and canonical-path authority, bypass the whole helper off POSIX, and add the real creator-race/adversarial tests before focused round 4. |
| R3-F9 | **Corrected.** The header, source list, and baseline now name two live normative deltas: timestamp grammar and `SQLiteRunner` creation. The rejected cleanup proposal is historical text only. |
| R3-F10 | **Accepted.** Rollback distinguishes code reversion from persistent mode restoration. Unit E has no new destructive implementation in this train. |
| R3-F11 | **Open and blocking durable guidance.** The overlapping uncommitted `docs/lessons.md` entry still teaches the rejected cleanup protocol. It must be corrected or discarded before related work lands; this plan review does not overwrite that concurrent change. |

## Independent Re-review Findings (Round 4 / Revision 5, 2026-08-06)

### Destructive-premise and experiment result

The owner decision changes the question. Cleanup is not trying to preserve work
that the command exists to delete. Therefore an idle holder, successful stale
write, or racing replacement is not a refusal condition. It is part of the
explicit destructive blast radius. This removes the need for revision 3's
disproved SQL transaction/checkpoint protocol, but it does not make concurrent
unlink safe or predictable.

The primary review ran 540 process-isolated diagnostic cases across macOS
SQLite 3.50.4, Linux/glibc SQLite 3.40.1, and Linux/musl SQLite 3.51.2. They
covered main-first and sidecars-first deletion, old/new write order, old/new
close order, stale-holder checkpoints, and simultaneous 1,000-row writes. In
those cases the stale holder's first write always succeeded, a replacement
generation could be created at the same path, the replacement database ended
healthy, and no burst write failed. Stale successful writes disappeared when
the old holder closed, while its unlinked files retained disk until close.

Those observations are **not** the contract. SQLite's
[upstream unlink warning](https://www.sqlite.org/howtocorrupt.html#unlink) says
old and replacement database generations can share pathname-derived
journal/WAL names and that unlink/rename behavior under open handles is
undefined and probably undesirable. A separate POSIX lock probe also showed
that unlinking a held phaselock path lets a replacement inode acquire an
independent lock. Revision 5 therefore documents undefined overlap rather than
promising the benign two-generation outcome observed in this sample.

### Focused independent findings and dispositions

| ID | Severity / confidence | Finding | Disposition |
|----|-----------------------|---------|-------------|
| E5-F1 | P1 / 10 | Main observation, temp enumeration, disappearance, and the backend boolean initially left third outcomes unspecified. | **Corrected.** Main inspection/validation is the zero-delete safety gate. Missing parent is empty. Other temp-enumeration errors are reported but fixed names are still attempted. Fixed names use direct unlink; main/enumerated temps retain observed status across disappearance. `True`/`False` is exact. |
| E5-F2 | P1 / 10 | Deleting `.lock` and `.vacuum.lock` can split active coordination even before a SQLite connection exists. | **Corrected.** Undefined overlap covers every active SimpleBroker operation/process using the target plus raw SQLite clients. Runner, phaselock, and vacuum-lock comments are Unit E owners. |
| E5-F3 | P2 / 9 | Calling failed paths “residual” overclaimed their state without a final check. | **Corrected.** One stderr diagnostic names failed observation/unlink attempts and says other entries may already be gone. Cleanup neither retries nor rolls back; partial failure exits nonzero. |
| E5-F4 | P2 / 9 | The firing matrix omitted unreadable main, observation failures, simultaneous failures, temp ordering/grammar, and CLI path-grammar boundaries. | **Corrected.** T-E now enumerates all branches, including ASCII-only temp grammar with non-ASCII near-misses, partial iteration, deterministic aggregation, `%` CLI coverage, and backend-only `?`/`#` validation cases. |

Independent SQLite lifecycle verdict after correction: **PASS — implementable
without inventing policy.** The reviewer ran the existing cleanup and
validation-lock slice (14 tests passed) and `bin/check-plan-context` (passed).
No product files were changed by review.

### Agent-facing interface review (Unit E)

| Principle | Revision-5 result |
|-----------|-------------------|
| 1. Context economy | Pass: one success/no-op status or one aggregate error; no state dump. |
| 2. Progressive disclosure | Pass: destructive global help → README/kernel summary → [SB-OPS-7] exact behavior. |
| 3. Self-explanatory names | Pass: help says “destructively delete”; the spec names every owned suffix. |
| 4. One identity | Pass: one expanded, resolved target owns every derived name. |
| 5. Derive derivable values | Pass: callers provide only the target; cleanup derives the closed namespace. |
| 6. No hidden setup | Pass: no session, marker, xattr, or prior quiescence step is required. |
| 7. Teach, do not reject | Pass with safety boundary: an existing foreign/invalid main is a true destructive conflict and is rejected before mutation. |
| 8. Every message carries its action | Pass under owner policy: the error names each failed attempt and incomplete best-effort result; manual response is possible, but cleanup itself performs no retry or rollback. |
| 9. Atomicity and recovery | Deliberate documented departure: multi-file deletion is non-atomic and irreversible. Backup and stopping all activity are the path to a predictable operator result. |
| 10. Trust boundary | Pass: validated main when present; explicit destructive authority for the bounded orphan namespace; no recursive or broad glob. |
| 11. Mental model | Pass: the CLI speaks in configured target state; storage-generation detail appears only in the SQLite limitation. |

Enumerable gates: [SB-OPS-7], the new operations inventory/registry row, CLI
exit/quiet/JSON behavior, every owned suffix, and every failure branch have
named firing tests in T-E.

## Finding Dispositions (Revision 5)

| ID | Disposition |
|----|-------------|
| R3-F1–R3-F2 | **Superseded, not disproved.** The revision-3 quiescence mechanism remains invalid. Revision 5 no longer seeks quiescence or treats another holder as a refusal condition. |
| R3-F3 | **Accepted as a documented destructive limitation.** Open-file unlink remains upstream-undefined and potentially corrupting. [SB-OPS-7] promises no concurrent storage, coordination, client, or file-recreation outcome. |
| R3-F4 | **Corrected.** Platform failures, URI-safe validation, resolved identity, backend bool compatibility, best-effort error aggregation, quiet/JSON/exit behavior, backup guidance, and the full owned namespace are explicit. |
| R3-F10 | **Corrected for restored E.** Cleanup deletion is an irreversible one-way door; code rollback restores nothing. |
| R3-F11 | **Open and blocking durable guidance.** The overlapping `docs/lessons.md` edit still teaches the rejected exclusive-transaction protocol and must be corrected or discarded before related work lands. |
| E5-F1–E5-F4 | **Accepted and corrected; focused independent re-review PASS.** Unit E is plan-ready. |

## Owner Disposition (Revision 6, 2026-08-07)

The deployment review found that Unit B's “security flaw” classification lacked
a concrete threat model. A containing directory that excludes unintended users
is the practical access boundary for the typical single-user deployment on
POSIX and Windows. A shared-directory deployment requires an operator-owned
policy across the main database, all SQLite companions, all SimpleBroker
coordination/maintenance files, and directory traversal/creation/removal rights.
Forcing one owner-only permission set would override that policy and the
proposed helper still did not define the whole namespace.

Disposition: drop Unit B's product-code and normative-API changes. Keep a
documentation correction in J that lists every associated file and states the
effective read/write file plus directory traversal/creation/replacement/removal
requirements for every intended writer, with POSIX and Windows forms named.
This resolves R3-F5–R3-F9 by removing their proposed behavior rather
than inventing answers for an unproven release blocker. The historical findings
remain evidence for why that implementation must not be revived piecemeal.

## Durable-Guidance Correction (2026-08-07 — resolves the last blocker)

The conflicting `docs/lessons.md` WAL/SHM entry (R3-F11) has been
replaced in place by its author (it was uncommitted, so no supersession
ceremony is owed). The corrected entry now records: (1) `PRAGMA
wal_checkpoint(TRUNCATE)` inside `BEGIN EXCLUSIVE` fails
`database table is locked` even uncontended; (2) WAL-mode
`BEGIN EXCLUSIVE` equals `BEGIN IMMEDIATE` and excludes only writers;
(3) WAL last-close cleanup is SQLite's internal main-file
`SQLITE_LOCK_EXCLUSIVE`, unreachable from SQL; (4) an enforceable
protective cleanup would need out-of-band lifetime coordination in its
own class-5 design; and (5) the shipped resolution is revision 5's
explicitly destructive contract with concurrent-storage outcomes
documented as undefined per upstream. **Deliberate deviation from
R3-F11's suggested wording:** the suggested "never unlink an open
SQLite database" is stated as the upstream-undefined fact plus the
revision-5 contract choice, not as a flat prohibition — a prohibition
would now conflict with the adopted destructive cleanup this plan
ships. The entry also records that its own first version taught the
disproved protocol hours after drafting (the lessons ledger is a
reviewable surface). Verification: `check-dom15-fixtures` and
`check-plan-context` green after the replacement. Owner confirmation
of this correction closes the final blocker; A, E, I, J, K0/K are then
implementation-ready.

## GSTACK REVIEW REPORT

| Review | Date | Runs | Status | Findings |
|--------|------|------|--------|----------|
| Focused independent passes | 2026-08-06 | CLI/interface, SQLite/lifecycle, Redis/release | BLOCKED (round 2) | Four P1 findings, all in retained SQLite Units B/E; A and I corrected in-review; K retained by owner decision. |
| Codex outside voice | 2026-08-06 | 1 time-boxed read-only pass | CORROBORATED | Independently reproduced fractional drift, live-WAL cleanup risk, and missing CI structure proof before timeout. |
| Eng re-review | 2026-08-06 | Round 2 complete | BLOCKED (revision 2+amendments) | Could implement A/I/J/K; could not safely implement B/E. R2-F1–R2-F4 dispositioned in revision 3 (Units B and E rebuilt: shared secure-create helper with split guarantee; SQLite-mediated cleanup with zero raw sidecar unlinks). |
| Focused independent passes | 2026-08-06 | Unit B, Unit E, holistic coherence | BLOCKED (round 3) | Eleven round-3 findings. Unit E's success path is disproved; Unit B retains security, identity, compatibility, and race gaps; a concurrent lessons edit promotes the rejected fix. |
| Codex outside voice | 2026-08-06 | 1 read-only focused pass | ISSUES FOUND | Corroborated the Unit E protocol failure/open-unlink risk and Unit B mode/authority gaps; one magic-validation false positive rejected. |
| Eng re-review | 2026-08-06 | Round 3 complete | BLOCKED (revision 3) | A/I/J/K remain plan-ready. E should leave this train; B needs a focused revision 4 and re-review. |
| Owner disposition | 2026-08-06 | Revision 4 | ACCEPTED SCOPE REDUCTION | Unit E and `[SB-OPS-7]` removed; J documents offline/main-file-only SQLite cleanup and possible residue. No marker, xattr, or lifecycle lock in 6.0.2. |
| Owner disposition | 2026-08-06 | Revision 5 | ACCEPTED DESTRUCTIVE CONTRACT | Revision-4 deferral superseded. Best-effort cleanup deletes the full known owned namespace; overlapping SimpleBroker/SQLite outcomes are undefined; failed deletion attempts report once on stderr, continue, do not retry or roll back, and exit nonzero. |
| Primary experimental/interface re-review | 2026-08-06 | 540 macOS/glibc/musl cases + 11-principle interface pass | PASS (Unit E after corrections) | Observed stale/replacement generations without promising them; upstream undefined boundary governs. Full allowlist, URI/path identity, result, docs, and test contracts rebuilt. |
| Independent SQLite lifecycle re-review | 2026-08-06 | Initial pass + 2 focused correction audits | PASS (Unit E) | Observation/disappearance accounting, active-operation overlap, failed-attempt wording, best-effort semantics, ASCII temp grammar, partial enumeration, docs ownership, and firing matrix align. Existing 14-test cleanup/validation slice and plan-context gate passed. |
| Owner deployment/threat-model review | 2026-08-07 | Unit B scope and real mode probes | ACCEPTED SCOPE REDUCTION | No concrete release-blocking threat was established. Forced `0600` and reopen normalization are dropped; J documents the complete operator-owned cross-user filesystem condition instead. |

**VERDICT: ENG BLOCKED overall, but Units B and E are dispositioned. Revision 5 resolves the cleanup
dispute with an explicit destructive, best-effort contract and complete owned
namespace. Revision 6 drops Unit B's unproven forced-mode change and makes its
deployment boundary documentation-only. The plan is not yet implementation-ready
because the conflicting durable lesson still teaches the rejected cleanup
protocol. A, E, I, J, K0/K are plan-ready;
C/D/F/G/H remain deferred.**

**UNRESOLVED BLOCKERS:**

- ~~Correct or discard the overlapping uncommitted `docs/lessons.md` WAL/SHM
  entry before it lands; it currently teaches the disproved Unit E protocol.~~
  **Resolved 2026-08-07** — corrected in place by its author; see
  `## Durable-Guidance Correction` above (including the recorded deliberate
  deviation from R3-F11's literal "never unlink" wording, which revision 5
  superseded). Awaiting owner confirmation; no other blockers remain.
