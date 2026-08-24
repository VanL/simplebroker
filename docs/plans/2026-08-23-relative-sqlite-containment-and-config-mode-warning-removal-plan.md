# Relative SQLite Containment and Config Mode Warning Removal Plan

Class: 5 — revises normative CLI target-failure and project-config diagnostic
behavior; the published CLI safety and compatibility triggers make the
hardening checklist mandatory.

Plan type: implementation with spec revision.

Status: completed — owner authorized the targeted landing commit after full
verification and independent completed-work review.

## Goal

Make ordinary relative legacy-SQLite CLI target validation fail closed when
SimpleBroker cannot establish the documented working-directory containment,
and pass the validated canonical target into command dispatch. Remove the
POSIX `.broker.toml` file-mode warning because effective file and parent-
directory permissions, ownership, and ACLs are the operating-system trust
boundary; retain the redacted inline-password warning. Preserve explicit
absolute targets, trusted project-config targets, Unix stream roles, JSON
errors, and every storage/delivery contract.

## Investigation Disposition Matrix

| Review claim | Disposition | Owning slice |
|--------------|-------------|--------------|
| Relative-path resolution failure falls back to an unchecked lexical path | Accepted: containment-required resolution failures become fatal before backend dispatch | Tasks 1–3 |
| A bare inner `pass` is acceptable availability recovery | Rejected: it makes a safety check best-effort and hides why the target could not be established | Tasks 2–3 |
| Validating a resolved path while dispatching the unresolved `BrokerTarget` is sufficient | Rejected for ordinary relative targets: dispatch receives the same canonical target that passed validation | Tasks 2–3 |
| Explicit absolute `-f` should be confined to `-d` | Rejected: an explicit absolute target is direct user authority and retains existing behavior | Ratified in proposed `[SB-CLI-2]` delta |
| Project-config targets should be confined to the project or log an exemption | Rejected: `.broker.toml` is a trusted anchor whose target may leave the project and traverse symlinks; routine exemption chatter would damage stderr composition | Ratified in proposed `[SB-CLI-2]` / `[SB-API-2]` deltas |
| SimpleBroker should defend against concurrent replacement in an attacker-writable directory | Rejected as an application-layer guarantee: effective Unix/Windows file and directory permissions and ACLs own that trust boundary | Ratified in proposed `[SB-CLI-2]` / `[SB-API-2]` deltas |
| Group/other-readable `.broker.toml` should warn or become a strict refusal | Rejected: one inode's POSIX mode bits cannot establish the effective path trust boundary; remove the mode warning and add no strict mode | Tasks 1, 2, and 4 |
| An inline backend password should still warn | Accepted: the parser directly observes this application-level choice and can warn without exposing the value | Tasks 2 and 4 |

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-2]
- `docs/specs/17-ops.md` [SB-OPS-7] (cleanup boundary that must not be
  conflated with ordinary command-target validation)
- `docs/specs/product-section-registry.md` (winning-owner rows for CLI I/O,
  Python/project-config helpers, and residual project-scoping guidance)
- `docs/guides/configuration.md` (project-target precedence, trusted-anchor
  rule, and effective filesystem-permission boundary)
- `docs/agent-kernel.md` (agent-facing target selection guidance)
- `docs/implementation/02-repository-map.md` (`_paths.py` ownership summary)
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `skills/interface-review/SKILL.md`

## Spec Baseline

- `d63e65523103229066de7531cb3b1183cd0f45c4` —
  `docs/specs/10-cli.md` and `docs/specs/16-python-library-api.md` at plan
  promotion time, after the overlapping maintainability work landed in
  `a490dcc` and the independent polling change landed in `d63e655`.
- Promotion baseline: Strategy-A spec delta against `d63e655`, SHA-256
  `c45de4a6540746c3ca32f5beae3570ec6dd82bc51558921e848d8648cb8c8f94`
  for `git diff d63e655 -- docs/specs/10-cli.md
  docs/specs/16-python-library-api.md`. `check-dom15-fixtures`,
  `check-plan-context`, `check-doc-paths`, and `git diff --check` passed at
  promotion. Runtime work is judged against this exact worktree delta until
  the owner lands it.

## Context and Key Files

### Current owners and behavior

- `simplebroker/cli.py::_validate_legacy_sqlite_target` owns ordinary legacy
  SQLite pre-dispatch validation. It calculates a local resolved `db_path`,
  validates parent/database state with that value, and returns `None`; the
  caller later dispatches the original frozen `BrokerTarget`.
- `simplebroker/cli.py::_resolve_legacy_sqlite_path` resolves the candidate and
  working directory and checks containment only on the successful path. It
  catches `RuntimeError` / `OSError`, reconstructs a lexical path without a
  containment check, and silently ignores a second resolution failure.
- `simplebroker/_paths.py::_resolve_symlinks_safely` calls
  `Path.resolve()` without an explicit `strict` argument. Its inner loop
  returns a partial path after read/resolve failure and does not reject depth
  exhaustion. Supported Python versions do not report symlink loops identically,
  so the helper must own a version-stable fail-closed result.
- `simplebroker/_paths.py::_validate_path_containment` compares a resolved
  database target with a resolved working directory. It is the existing
  containment owner and should remain single-owned; do not duplicate its
  predicate in `cli.py`.
- `simplebroker/_constants.py::_validate_safe_path_components` owns lexical
  rejection of `.` / `..` and dangerous filename components. That is distinct
  from physical symlink containment. Its current prose overstates the helper as
  preventing a general class of attacks.
- `simplebroker/_project_config.py::_warn_for_insecure_project_config` combines
  two unrelated observations: a target string containing a recognizable
  password, and any POSIX group/other mode bit. The latter warning says
  "group/other-readable" even though the mask includes write and execute bits
  and ignores the effective directory/ACL boundary.
- `simplebroker/_targets.py::BrokerTarget` is a frozen public descriptor. Do
  not add a public field or change serialization. A prepared internal target
  may be made with `dataclasses.replace`, preserving backend options and
  metadata while changing only the relative SQLite target string used for
  dispatch.
- `simplebroker/cli.py::_prepare_command_target` and `_prepare_dispatch` own
  error translation before `_dispatch_command`. The prepared-target result
  must pass through this existing owner without creating a second dispatcher.
  However, `_main` currently calls `_run_target_action` before that preparation:
  global `--status` reaches `commands.cmd_status` and `--vacuum` /
  `--compact` reaches `_run_vacuum`, both of which can open `DBConnection`.
  The implementation must move one shared preparation result ahead of these
  target-opening actions while leaving `init` and cleanup in their separately
  specified owners. Plain errors remain exit `1`; after JSON mode is
  recognized, `[SB-CLI-4]` still requires exactly one JSON object on stderr.
- `docs/guides/configuration.md` already says `.broker.toml` is a trusted
  project anchor and that operators own effective permissions on the database,
  companion files, and containing directory. It also promises the mode warning;
  that one sentence must be removed and replaced with the complete config-file
  boundary.
- Weft primarily supplies resolved Python targets and does not rely on the
  legacy CLI path validator or the mode warning. It remains a required
  read-only compatibility check because it is the primary downstream and
  consumes SimpleBroker configuration contracts.

### Concurrent-plan and dirty-tree gate

`docs/plans/2026-08-23-maintainability-and-isolation-remediation-plan.md` owns
overlapping edits in `simplebroker/cli.py`, `simplebroker/_paths.py`,
`simplebroker/_constants.py`, `tests/test_path_security.py`, both winning specs,
`CHANGELOG.md`, and the plan index. Treat that ownership as unresolved until
`git log` identifies its landing commit and the exact overlapping diff has been
rebased into this plan's baseline; an index status alone is not landing proof.

Before Task 1 or any runtime edit:

1. inspect `docs/plans/README.md`, `git status --short`, and the exact diff for
   every file in the next slice;
2. do not edit an overlapping file while the maintainability plan has an
   in-flight slice;
3. wait for that slice to land, explicitly pause it, or rebase this plan after
   its owner records a stable integration point;
4. re-read the post-integration `cli.py` preparation, status/vacuum, and
   dispatch flow plus the `_paths.py` helper before applying the proposed
   delta;
5. if the containment owner, parser/dispatch flow, spec paragraph, or path-test
   seam changed materially, append the changed assumption to the Review Log
   and obtain scoped re-review before promotion; and
6. stage and review only explicit files belonging to this plan. Never discard,
   reset, format, or absorb another plan's changes.

### Files to modify

- Runtime:
  `simplebroker/cli.py`, `simplebroker/_paths.py`,
  `simplebroker/_constants.py`, `simplebroker/_project_config.py`.
- Primary tests:
  `tests/test_symlink_security.py`, `tests/test_paths_coverage.py`,
  `tests/test_project_config.py`, `tests/test_status_command.py`,
  `tests/test_vacuum_compact.py`, and the narrow CLI contract/JSON test file
  selected after the concurrent-plan rebase (`tests/test_cli_contract_sb_cli.py`
  or an existing target-validation module). Add no new test module unless the
  existing files cannot keep contract and helper proofs legible.
- Winning contracts:
  `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`.
- Agent/human guidance and implementation ownership:
  `docs/guides/configuration.md`, `docs/agent-kernel.md`,
  `docs/implementation/02-repository-map.md`, `CHANGELOG.md`.
- Plan evidence:
  this file and `docs/plans/README.md`.

Do not modify `simplebroker/_targets.py`, `simplebroker/project.py`, backend
plugins, storage code, cleanup code/tests/specification, vacuum implementation
below its CLI target-preparation seam, README project-scope catalog, or Weft
unless an independently reproduced compatibility failure forces a replan and
owner approval.

### Required comprehension gate

Before runtime edits, the implementer records answers in the Execution Log. A
wrong or missing answer blocks implementation until the cited owner is reread.

1. **Which targets require working-directory containment?** Expected answer:
   ordinary relative legacy-SQLite CLI targets. An explicit absolute `-f` is
   direct user authority, and a `.broker.toml` target is a trusted developer
   anchor that may leave the project and traverse symlinks.
2. **What is wrong with the current fallback?** Expected answer: when physical
   resolution fails, it substitutes a lexical `resolved_working_dir / file`
   value without re-establishing physical containment; a second failure is
   swallowed. Safety validation therefore becomes best-effort precisely on
   its error path.
3. **Why must validation return a prepared target?** Expected answer: a local
   canonical `Path` used only for checks does not control the later backend
   open. The ordinary relative target passed into dispatch and the
   `--status` / `--vacuum` target actions must contain the same canonical
   string that passed containment. The original user spelling may remain
   available only in invocation arguments for diagnostics. `init` and cleanup
   retain their separately specified target paths.
4. **Does canonical dispatch defend an attacker-writable directory?** Expected
   answer: no. It aligns check and use under the stable-path contract, but the
   operating system's effective file and directory permissions, ownership, and
   ACLs own concurrent replacement trust. This plan adds no descriptor-relative
   filesystem API and claims no hostile-directory guarantee.
5. **Which project-config warning remains?** Expected answer: only the
   redacted warning when the target string itself contains a recognized inline
   password. SimpleBroker does not inspect file mode, parent permissions,
   ownership, or ACLs and does not add a strict config mode.

## Invariants and Constraints

1. Lexical `.` / `..` rejection remains separate from physical symlink
   containment, retains its current validation order, and continues to apply to
   non-project legacy targets.
2. An ordinary relative legacy-SQLite target reaches backend dispatch only
   after its canonical candidate and working directory have been resolved and
   `_validate_path_containment` has accepted them. Resolution uncertainty is
   fatal and exits `1`; no fallback path is opened.
3. The prepared `BrokerTarget` changes only the target string used for ordinary
   relative legacy-SQLite command dispatch. Backend name/options, project
   metadata, public type shape, serialization, and repr/redaction remain
   unchanged.
4. Explicit absolute `-f` behavior remains unchanged, including symlink use and
   the existing `-d` consistency rule. Project-config targets retain trusted-
   anchor behavior and may be absolute, parent-relative, remote, or symlinked.
5. A valid dangling symlink whose resolved destination is inside the selected
   directory remains usable for creating a new SQLite database. Missing final
   database files and one-level compound configured names remain supported.
6. Symlink loops, depth exhaustion, and inner read/resolve failures do not
   return partial paths. A readable dangling final symlink whose non-strict
   destination is an ordinary missing path inside the selected directory is a
   supported creation target, not an unresolved-loop failure. The helper emits
   no secret-bearing target content beyond the already-safe local SQLite path
   diagnostic.
7. Target-preparation failures happen before broker/backend command dispatch
   and before `--status` or `--vacuum` / `--compact` opens the target. `init`
   and cleanup retain their separately specified ordering. Existing safe
   directory preparation may retain its documented behavior; do not claim
   rollback for filesystem setup already performed before failure.
8. Plain stderr, quiet-mode error visibility, JSON error shape/code vocabulary,
   stdout emptiness, exit `1`, and traceback suppression remain `[SB-CLI-*]`
   compatible. Error prose is not frozen, but it must name the failed relative
   containment action and tell the caller to choose a resolvable in-directory
   path or an intentional explicit absolute target.
9. Cleanup stays governed by `[SB-OPS-7]`; this plan does not change its frozen
   namespace, symlink unlink behavior, diagnostics, or non-atomic semantics.
10. Project-config loading keeps warning without exposing a recognized inline
    password. A secret-free `0644` config and the same config under any other
    mode produce no permission diagnostic. No mode, ownership, directory, or
    ACL enforcement is added.
11. No new environment key, CLI flag, public exception, public target field,
    dependency, persistence state, backend handshake, cleanup lifecycle, or
    background work is introduced.
12. Stop and re-plan if correct cross-version resolution requires rejecting a
    currently supported in-directory dangling symlink, changing absolute or
    project-target behavior, adding a public target representation, or
    promising protection in attacker-writable directories.

## Rollback, Rollout, and One-Way Doors

There is no storage migration, format change, or new destructive operation.
The runtime and contract changes can be reverted as one slice before
publication. The mode-warning removal is independently revertible in code, but
must not be restored without also revising the ratified filesystem-boundary
text and reproducing a benefit that outweighs its false conclusions.

Rollout order:

1. clear the concurrent-plan gate and record the integration baseline;
2. independently review this plan and exact spec delta;
3. promote the Strategy-A spec text and record its rerunnable identifier;
4. write RED contract tests, then implement the path and warning slices;
5. run SimpleBroker and read-only Weft compatibility gates;
6. reconcile guidance, mappings, changelog, and firing evidence;
7. obtain independent completed-work review and stop before publication.

After publication, reverting fail-closed containment would restore a known
safety/correctness defect and requires an explicit owner decision plus a
corrective release. Reintroducing the mode warning would likewise contradict
the published trust boundary. The release owner decides SemVer treatment; this
plan neither chooses a version nor publishes packages.

There are no one-way storage doors. Post-release positive signals are: ordinary
new relative databases and in-directory symlinks continue to work; looping,
unreadable, depth-exhausted, or outside relative targets fail once with exit
`1` and no backend effect;
absolute and project targets show no regression; inline-password warnings stay
redacted; ordinary readable secret-free project configs stop producing the
removed warning; and Weft's configuration/Queue integration remains green.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. Task 1
applies the exact normative text below to the active specs and adds the live
Related Plans backlinks without claiming implementation/test mappings that do
not yet exist. Task 5 adds reciprocal implementation and firing-evidence links.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | [SB-CLI-2], Verification, Related Plans |
| `docs/specs/16-python-library-api.md` | A | [SB-API-2], Verification, Related Plans |

### `[SB-CLI-2]` — insert after the invalid-environment paragraph

> For an ordinary relative legacy-SQLite target, the CLI must establish the
> target's physical containment within the selected working directory before
> backend command dispatch or a target-opening `--status`, `--vacuum`, or
> `--compact` action. If path or symlink resolution cannot establish
> that containment, the invocation emits one actionable error and exits `1`;
> it does not open a lexical fallback. The backend receives the same canonical
> target string that passed containment. Once argument parsing has established
> JSON mode, this failure uses `[SB-CLI-4]`'s JSON error object; otherwise it is
> a plain stderr error. Stdout remains empty and no traceback is shown.
>
> An explicitly supplied absolute `-f` target and a trusted project-config
> target are intentionally outside working-directory containment. Project
> targets may leave the project and traverse symlinks. These pathname checks
> assume the selected path and its directories are protected by the
> operating-system permissions and ACLs chosen by the operator; they do not
> claim protection against concurrent replacement in a directory another
> principal may modify.

> `init` and `[SB-OPS-7]` cleanup retain their separately specified preparation
> and path behavior.

During Task 5, append `simplebroker/_paths.py` to `[SB-CLI-2]`'s implementation
mapping and add exact firing node IDs for fail-closed resolution, canonical
command/status/vacuum dispatch, missing-target creation,
in-directory/outside/loop symlinks, absolute/project exemptions, plain stderr,
and JSON stderr.

### `[SB-API-2]` — insert after the project-config helper list

> Project configuration is a trusted developer input. When the configured
> target string contains a recognized inline password, project-config loading
> emits a redacted advisory warning that does not include the password.
> SimpleBroker does not inspect, warn on, or enforce project-config file mode,
> ownership, parent-directory permissions, or ACLs. Confidentiality and
> integrity of the config path are governed by the effective operating-system
> permissions across the file and its containing directories.

During Task 5, add `simplebroker/_project_config.py` to `[SB-API-2]`'s mapping
if it is not already covered by `simplebroker/project.py`, and record the exact
tests proving inline URL/conninfo warnings are redacted and a readable
secret-free config emits no permission warning.

### Related Plans and traceability

During promotion, add this plan under each touched spec's `## Related Plans`.
Do not remove existing plan links or firing evidence. Task 5 adds or replaces
test-node claims only after the named test exists and passes.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Agent-Facing Interface Review

Surface: `broker` CLI target selection and its matching project-config helper
diagnostics. Promotion baseline: `d63e655`. Implemented delta: the promoted
Strategy-A text above. File-line evidence below describes the verified
implementation worktree.

| Principle | Baseline/proposed disposition | Evidence |
|-----------|-------------------------------|----------|
| 1. Context is the scarcest resource | Met: one failure record, no target dump or extra status chatter | `docs/specs/10-cli.md` `[SB-CLI-2]`; `simplebroker/cli.py:1332-1338` |
| 2. Progressive disclosure | Met: `-f` / `-d` stay the immediate surface; the configuration guide owns trust detail | `simplebroker/cli.py:255-275`; `docs/guides/configuration.md:229-253` |
| 3. Self-explanatory names | Met: no new flag or policy name; the error names relative containment and recovery | proposed `[SB-CLI-2]` delta |
| 4. One identity per thing | Met: validation returns a narrowly replaced target whose canonical string feeds command dispatch, status, and vacuum | `simplebroker/cli.py:1260-1313`, `simplebroker/cli.py:1762-1784` |
| 5. Derive what is derivable | Met: the CLI derives containment from the selected working directory and target; caller supplies no public policy bit | `simplebroker/cli.py:1270-1297` |
| 6. No hidden session setup | Met: ambient working directory remains inspectable; explicit absolute and project selection are documented | `docs/agent-kernel.md:184-197`; `docs/guides/configuration.md:229-253` |
| 7. Teach, don't reject | Justified departure: unresolved containment is unsafe to normalize; the failure tells callers to use a resolvable in-directory target or explicit absolute target | `simplebroker/cli.py:1332-1338` |
| 8. Every message carries its action | Met: the error gives both supported recovery paths and preserves plain/JSON translation | `simplebroker/cli.py:1332-1338`, `simplebroker/cli.py:1639-1658` |
| 9. Atomic writes with recovery | Met at this seam: one preparation result precedes status, vacuum, and ordinary command dispatch. Multi-writer merge is not applicable | `simplebroker/cli.py:1752-1784` |
| 10. Draw the trust boundary | Met: the mode heuristic is gone; guidance assigns effective path permissions/ACLs to the OS/operator while retaining the observable inline-password advisory | `simplebroker/_project_config.py:34-41`, `docs/guides/configuration.md:567-582` |
| 11. Wire format matches the mental model | Met: relative means contained; absolute/project means explicit trust; plain/JSON error roles stay unchanged | `docs/specs/10-cli.md` `[SB-CLI-2]`; `docs/specs/16-python-library-api.md` `[SB-API-2]` |

Findings:

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-1 | P2, resolved | `simplebroker/cli.py:1260-1341`, `simplebroker/cli.py:1639-1784` | Relative resolution now fails closed; the canonical checked target is carried into command, status, and vacuum paths before opening | Closed by Tasks 2–3 and exact firing tests in `[SB-CLI-2]` |
| IR-2 | P2, resolved | `simplebroker/_project_config.py:34-41` | The mode heuristic is removed; only the redacted inline-password advisory remains | Closed by Tasks 2 and 4 plus `[SB-API-2]` firing tests |

Ratified judgment calls: absolute `-f` and trusted project targets remain
unconfined; no routine exemption warning is added; no strict config mode,
descriptor-relative filesystem mechanism, or permission/ACL evaluator is
introduced.

Verdict: **no remaining interface-review blocker**. IR-1 and IR-2 are resolved
in the verified worktree; independent completed-work review remains a separate
DOM-11 gate. Runbook feedback: no change recommended; the existing checklist's
principles 4, 8, and 10 exposed the actual identity and trust-boundary defects
without adding ceremony.

## Dependency-Ordered Tasks

1. **Clear overlap, independently review, and promote the contract.**
   - Files: this plan, `docs/plans/README.md`, `docs/specs/10-cli.md`,
     `docs/specs/16-python-library-api.md`.
   - Execute the concurrent-plan gate. Record the stable integration commit
     and rebase the baseline/delta if overlapping prose or owners moved.
   - Send the full plan, exact delta, current code/tests, program theory, and
     active maintainability plan to an independent reviewer. Resolve every
     blocking point in the Review Log.
   - Apply the exact Strategy-A text and live plan backlinks. Do not claim
     future mappings or tests. Run document gates and record the promotion
     baseline identifier.
   - Stop if review disputes the OS permission boundary, requires changing
     absolute/project behavior, or finds the prepared-target shape would alter
     public `BrokerTarget` serialization.
   - Done: reviewed specs are the sole implementation target and the promotion
     identifier is rerunnable.

2. **Write RED path and diagnostic contract tests.**
   - Files: `tests/test_symlink_security.py`, `tests/test_paths_coverage.py`,
     `tests/test_project_config.py`, and the post-rebase CLI contract test
     owner.
   - Use real temporary directories, real symlinks where supported, real
     SQLite, and the subprocess CLI for public behavior. Add a real self-loop
     and/or bounded loop proving one clean exit-`1` failure across supported
     platforms that permit symlinks. Preserve the current in-directory
     dangling-symlink creation proof and outside-chain rejection proof.
   - Add a focused helper/caller test that injects only the resolution failure
     needed to prove `_resolve_legacy_sqlite_path` no longer returns a fallback;
     do not mock containment or backend dispatch in the black-box proof.
   - Add a narrow preparation/dispatch seam assertion that the target delivered
     to `_dispatch_command`, `commands.cmd_status`, and `_run_vacuum` is the
     validated canonical relative target. These internal assertions support,
     but do not replace, real CLI filesystem tests.
   - Add plain and recognized-JSON failure cases: exit `1`, empty stdout,
     exactly one actionable stderr diagnostic, no traceback, and no outside
     database mutation.
   - Replace the mode-warning test with a secret-free readable-config test that
     expects no stderr. Retain both URL and conninfo inline-password tests and
     their secret non-disclosure assertions.
   - Record the expected RED failures. Stop if a reproducible test requires an
     attacker-writable-directory race; that is outside the contract and must
     not be smuggled in as acceptance criteria.
   - Done: failures map exactly to fallback, discarded prepared target, and mode
     warning behavior.

3. **Fail closed and carry one prepared relative target into every ordinary
   target-opening path.**
   - Files: `simplebroker/_paths.py`, `simplebroker/cli.py`,
     `simplebroker/_constants.py`, focused path/CLI tests.
   - Make `_resolve_symlinks_safely` explicit about non-strict final-leaf
     support while treating outer/inner resolution errors, unreadable or
     looping chains, and depth exhaustion as failures. A readable dangling
     final symlink whose destination is a missing ordinary path inside the
     selected directory remains valid. Do not add a platform dependency or use
     private OS APIs.
   - Remove `_resolve_legacy_sqlite_path`'s fallback. Translate resolution
     uncertainty into the existing target-preparation error path with
     actionable recovery text and no secret interpolation.
   - Finalize any supported compound target before canonical containment.
     Validate with the existing `_validate_path_containment`; do not duplicate
     its predicate.
   - Refactor `_validate_legacy_sqlite_target` / `_prepare_command_target` /
     `_prepare_dispatch` and `_main` ordering to return and carry one prepared
     `BrokerTarget` for ordinary relative legacy SQLite commands plus
     target-opening `--status` and `--vacuum` / `--compact`. Run `init` and
     cleanup through their existing separately specified path before shared
     preparation; run shared preparation once before status/vacuum or command
     dispatch. Use `dataclasses.replace` or an equally narrow internal
     construction; do not mutate `args`, add a public field, or create a second
     dispatcher. Absolute, project, non-SQLite, init, cleanup, and pre-target
     actions otherwise retain their present owners.
   - Narrow `_validate_safe_path_components` prose to lexical component
     validation. Update no other validation semantics.
   - Stop if the active CLI refactor has changed the single dispatch owner, if
     canonicalization changes a legitimate dangling-symlink target, or if
     `BrokerTarget` public serialization would change.
   - Done: Task 2 path and CLI tests pass on the current interpreter; existing
     project/absolute/cleanup suites remain green; status/vacuum tests prove no
     target open precedes preparation.

4. **Remove only the project-config mode warning and align guidance.**
   - Files: `simplebroker/_project_config.py`,
     `tests/test_project_config.py`, `docs/guides/configuration.md`,
     `docs/agent-kernel.md`, `docs/implementation/02-repository-map.md`,
     `CHANGELOG.md`.
   - Rename/split the current warning helper so it owns only recognized inline
     passwords. Remove the POSIX mode branch and now-unused `os` import. Do not
     replace it with write-bit, ownership, directory, ACL, Windows, or strict-
     mode checks.
   - Preserve secret redaction and the `BROKER_BACKEND_PASSWORD` recovery hint.
     Do not move the remaining warning to `warnings.warn()` or redesign the
     library diagnostic channel in this plan.
   - Replace the configuration-guide warning promise with the exact effective-
     path boundary. Add one compact agent-kernel sentence only if needed to
     keep the relative/absolute/project mental model self-contained. Change the
     repository-map description from general "path security" to the narrower
     implemented validation/discovery responsibility.
   - Add Unreleased entries for the fail-closed target correction and removed
     mode warning. Do not select a release version.
   - Stop if removing the mode warning exposes another public document or
     downstream test that treats it as enforcement rather than advisory; record
     and re-review the contract instead of adding replacement heuristics.
   - Done: project-config tests pass; only inline target passwords warn; docs
     state the OS/operator boundary without claiming application enforcement.

5. **Reconcile traceability and verify downstream compatibility.**
   - Files: touched specs/guidance, this plan, and exact test mappings only.
   - Add real passing node IDs to `[SB-CLI-2]` and `[SB-API-2]` verification,
     plus reciprocal implementation mappings/backlinks. Preserve every existing
     mapping unless the named test was actually replaced.
   - Run the interface-review checklist against the implemented CLI delta.
     Update the findings/verdict and runbook-feedback line; do not call the
     surface integration-ready while IR-1 or IR-2 remains open.
   - Inspect Weft's current SimpleBroker target/config callsites. Run its
     config integration test against this checkout with
     `(cd ../weft && uv run --with-editable ../simplebroker pytest -q
     tests/system/test_constants.py)`. The command is read-only with respect to
     source; disclose the existing Weft dirty tree and do not edit it.
   - Stop if Weft depends on the removed stderr warning or receives a changed
     public target/serialization shape. Record exact callsites and obtain owner
     direction; do not patch Weft as part of this plan.
   - Done: contract graph and interface verdict are closed, and downstream
     evidence is green or an explicit blocker is recorded.

6. **Run full verification, independent completed-work review, and closure.**
   - Run every final command below from the candidate tree, plus the supported-
     interpreter matrix in CI or equivalent managed environments. Re-run the
     affected slice after any fix.
   - Obtain an independent completed-work review of the full explicit-file
     diff, promoted spec baseline, runtime path, tests, config boundary, and
     downstream evidence. Reproduce and disposition every finding.
   - Evaluate whether the interface-review skill/runbook needs improvement.
     Promote nothing without a second independent surface; record "no change"
     when appropriate.
   - Close the index row only after implementation, specs/docs, all gates,
     independent review, and an owner landing commit are recorded. Never stage
     another plan's changes.
   - Done: reviewer PASS, zero unresolved deviation, green gates, clean
     explicit-file diff against the landing commit, and Status Index row
     `completed`.

## Testing Plan

Red-green TDD is required for the runtime and warning changes. The permitted
exit is a platform that cannot create symlinks; its black-box symlink cases may
skip under the existing capability gate, but helper-level fail-closed tests and
the supported POSIX interpreter matrix must still fire.

Primary behavior matrix:

- **Relative target success:** new database file, one-level compound default,
  existing database, in-directory symlink, and in-directory dangling symlink
  all dispatch successfully to the canonical contained target.
- **Relative target refusal:** outside symlink, parent symlink, symlink chain,
  loop/depth exhaustion, and injected resolution error fail with exit `1`,
  empty stdout, one actionable diagnostic, no traceback, and no backend effect
  outside the selected directory. The same refusal holds for global status and
  vacuum/compact before their connection/plugin paths run.
- **Authority exemptions:** explicit absolute targets and project-config targets
  outside the working/project directory retain existing success and target
  identity behavior. No exemption warning is added.
- **Wire behavior:** plain mode uses the shared error dialect; recognized JSON
  mode emits exactly `[SB-CLI-4]`'s object and code vocabulary; quiet does not
  suppress errors.
- **Prepared target:** the ordinary relative `BrokerTarget` handed to command
  dispatch, status, and vacuum contains the canonical checked target. Public
  `BrokerTarget` fields, serialization, repr/redaction, and non-SQL target
  behavior do not change.
- **Project config:** inline URL and conninfo passwords warn without exposing
  the value; secret-free readable configs emit no mode warning; env-supplied
  passwords remain silent; project target precedence and trust-anchor tests
  remain green.
- **Cleanup exclusion:** `[SB-OPS-7]` symlink namespace and cleanup behavior
  remain unchanged.
- **Downstream:** Weft config integration remains green against the local
  SimpleBroker checkout and no source edit is required.

Use real filesystem entries, subprocess CLI, and SQLite for public-path proofs.
Do not mock `_validate_path_containment`, `BrokerDB`/SQLite, argument parsing,
JSON serialization, or project-config parsing in those proofs. Narrow
monkeypatching is allowed only to force an otherwise version-dependent
resolution exception and to capture the prepared target at the internal
dispatch boundary; those tests supplement real symlink/CLI cases.

## Verification and Gates

Per-task RED/GREEN results and exact node IDs belong in the Execution Log.
Final minimum, adjusted only for test ownership changes after the concurrent
plan lands:

```bash
uv run pytest -q tests/test_symlink_security.py tests/test_paths_coverage.py tests/test_project_config.py tests/test_path_security.py
uv run pytest -q tests/test_cli_contract_sb_cli.py tests/test_cli_main.py tests/test_status_command.py tests/test_vacuum_compact.py tests/test_cleanup.py tests/test_target_redaction.py
uv run pytest -q
uv run ruff check simplebroker tests
uv run ruff format --check simplebroker tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
python3 bin/check-doc-paths
bin/coalesce-check
git diff --check
(cd ../weft && uv run --with-editable ../simplebroker pytest -q tests/system/test_constants.py)
```

Supported-interpreter proof runs the focused path/config group on Python 3.11,
3.12, 3.13, and 3.14 through the repository's existing CI/tox-equivalent
driver. If no single local driver owns that matrix, use the CI matrix and
record job URLs/identifiers; do not claim cross-version completion from one
interpreter. Windows must prove the non-symlink helper/error and ordinary path
cases; symlink cases follow the existing capability skip.

Apply adversarial acceptance probes to: ordinary success; malformed/loop path;
plain versus JSON failure; quiet error; missing final target; outside symlink;
explicit absolute target; trusted project target; and config with/without an
inline password. Each probe records exit class, stdout, stderr shape, target
side effects, and traceback absence.

Before any later publication, rerun the full gate from the exact release
identifier. This plan does not publish or authorize a release.

## Independent Review Loop

Plan review and completed-work review use a different agent family when
available. The plan reviewer receives this file verbatim, the exact
`## Proposed Spec Delta`, `docs/program-theory.md` `[THEORY-1/4]`, both
baseline specs, configuration guide security section, current `cli.py` target
preparation/dispatch/status/vacuum path, `_paths.py`, `_project_config.py`,
focused tests, Weft callsite inventory, and the overlapping maintainability
plan.

Review stance:

> Could you implement this confidently and correctly after Strategy-A
> promotion? Existence-check every named surface. Look for a relative path that
> can still reach command/status/vacuum target opening without containment, a
> partial/loop path treated as resolved, loss of missing-file or readable
> dangling-final-symlink support, accidental
> confinement or chatter for absolute/project targets, a public BrokerTarget
> shape change, a permission heuristic reintroduced under another name, inline
> password disclosure, JSON/plain error drift, cleanup overlap, weak mocked
> proof, concurrent-plan collision, or ceremony that does not protect a real
> boundary. Recommend removal as readily as additions. Answer PASS or BLOCKED
> under the DOM-11 gate questions.

The author reproduces each finding in the append-only Review Log and records an
accepted, rejected, or out-of-scope disposition with evidence. A BLOCKED result
or inability to answer the five comprehension questions blocks promotion.
Revisions that change authority, target classes, public shape, trust boundary,
or blast radius require scoped re-review against the reviewed delta.

Reader testing uses a fresh-context reviewer to answer the five comprehension
questions from this plan alone. Any wrong answer blocks handoff until the
relevant section is rewritten.

## Out of Scope

- Adding `BROKER_STRICT_CONFIG`, another environment key, a CLI strict flag, a
  config policy object, or a `broker config check` command.
- Evaluating or enforcing file modes, ownership, groups, parent-directory
  permissions, POSIX ACLs, Windows ACLs, mount policy, or process umask.
- Descriptor-relative `openat` / no-follow APIs, inode pinning, or protection
  against concurrent replacement inside an attacker-writable directory.
- Confining, warning on, or changing explicit absolute or trusted project-
  config targets.
- Removing or redesigning the inline-password warning, changing its stderr
  channel, or replacing it with `warnings.warn()`.
- Changing `BrokerTarget`'s public fields, serialization, repr, plugin
  handshake, or target precedence.
- Changing cleanup or init path semantics, vacuum behavior below its CLI
  target-preparation seam, discovery depth/mount behavior, database schema,
  SQLite transactions, delivery semantics, or backend code.
- Broad CLI refactoring, path-helper cleanup beyond the fail-closed invariant,
  new dependencies, release/version selection, publishing, or editing Weft.
- Starting the deferred coalescing sweep.

## Assumptions and Open Questions

- **Prepared-target seam:** the plan chooses a narrow internal prepared
  `BrokerTarget`, not a new public target type or second dispatch path. The same
  prepared result must feed command dispatch, status, and vacuum before any of
  them opens the target. If the overlapping CLI work lands a better single-
  owner result channel, reuse it and obtain scoped review rather than restore
  the discarded canonical path.
- **Cross-version resolver:** valid missing/dangling final targets are a
  compatibility requirement. The implementation may tighten
  `_resolve_symlinks_safely` or replace its internals, but must not simply use
  `strict=True` for the full path and break creation.
- **Error code:** target-preparation failures retain the existing ordinary
  `ERROR` JSON code unless review proves the current public path already uses
  `INVALID_ARGUMENT`; changing the closed vocabulary or category is outside
  this plan.
- **Guide ownership:** the configuration guide is the residual owner for full
  project-scope/security explanation. Root README already points to it and
  states effective permission ownership; change README only if final
  traceability inspection finds a stale warning claim.
- **Release:** the owner decides whether the stricter failure and removed
  advisory warrant patch or minor treatment. Implementation may proceed after
  spec promotion; publication may not.

## Consulted Surfaces Declaration

At authoring, the plan consulted program theory `[THEORY-1/4]`, the decision
hierarchy and engineering principles, `[DOM-5]` / `[DOM-15]`, the writing-plan,
hardening, testing, traceability, adversarial-probe, and agent-interface
runbooks, the interface-review skill, `[SB-CLI-*]`, `[SB-API-*]`, `[SB-OPS-7]`,
the product registry, configuration guide, agent kernel, repository map,
current runtime owners/direct callers/tests, changelog, active overlapping
plans/diffs, and read-only Weft usage. Implementation must append any newly
consulted surface or explicit waiver before closure.

## Fresh-Eyes Review Checklist

- Every named file, callable, spec code, test module, and command exists after
  the concurrent-plan rebase.
- Relative targets alone require containment; absolute/project authority does
  not drift.
- Resolution error, partial result, loop, and depth exhaustion are fatal, while
  missing final files and readable in-directory dangling symlinks whose
  destination is a missing ordinary path still work.
- The checked canonical target is the one dispatched without a public target
  shape change or parallel dispatcher, including status and vacuum paths that
  currently run before preparation.
- Error exit, stdout/stderr, JSON, quiet, traceback, and action guidance remain
  explicit and enumerable.
- The mode warning is removed, inline-password redaction remains, and no
  replacement permission heuristic appears.
- OS/operator ownership of the complete file-and-directory permission/ACL
  boundary is stated without promising hostile-directory protection.
- Cleanup, storage, delivery, target precedence, backend plugins, and Weft
  remain outside or explicitly verified unchanged.
- Rollout, rollback, SemVer ownership, post-release signals, downstream proof,
  anti-mocking guidance, stop gates, deviations, and two independent reviews
  are present.
- No task or abstraction exists only to satisfy review form.

## Execution Log

- 2026-08-23 plan authoring: classified Class 5 with mandatory hardening because
  normative `[SB-CLI-2]` / `[SB-API-2]` behavior and a published CLI safety
  boundary change. Baseline `32210e58`; no implementation or spec promotion
  performed. The named maintainability plan owns overlapping files, so Task 1's
  concurrency gate blocks runtime edits until its landing baseline is proved
  and this plan is rebased.
- 2026-08-23 independent plan review correction: the reviewer found that
  `_run_target_action` opens relative targets for `--status` and `--vacuum`
  before `_prepare_dispatch`. The plan now routes one prepared target ahead of
  those actions, preserves `init` and cleanup as explicit exceptions,
  distinguishes valid readable dangling-final symlinks from loop/read/depth
  failures, and requires promotion-baseline refresh of interface line evidence.
- 2026-08-23 scoped re-review: the same reviewer verified the accepted
  correction across Context, invariants, exact `[SB-CLI-2]` delta, tasks,
  testing, verification, Out of Scope, symlink compatibility, and line-evidence
  refresh. Result: PASS; the plan is review-ready and the Status Index may move
  to `active`. Task 1's landing/rebase gate still blocks implementation.
- 2026-08-23 Task 1: overlap gate cleared after maintainability landed in
  `a490dcc` and polling landed in `d63e655`. Rebased the plan and promoted the
  independently reviewed Strategy-A `[SB-CLI-2]` / `[SB-API-2]` delta against
  `d63e655`; promotion digest and green document gates are recorded in Spec
  Baseline. Runtime implementation may begin with red-green TDD.
- 2026-08-23 Tasks 2–4 RED/GREEN: real relative symlink-loop command and
  status probes first timed out/reached SQLite on the baseline; the canonical
  command-dispatch seam first received the unresolved alias; the configured
  compound-target seam first returned the uncanonicalized path; and the
  secret-free `0644` project config first emitted the old mode warning. The
  implementation now treats outer/inner/depth resolution uncertainty as
  fatal for containment-required targets, finalizes the candidate before
  containment, returns one narrowly replaced `BrokerTarget`, prepares status
  and vacuum before target opening, and removes only the mode branch. Focused
  path/config/CLI/status/vacuum/cleanup/redaction groups passed, including the
  exact `[SB-CLI-2]` and `[SB-API-2]` nodes recorded in the winning specs.
- 2026-08-23 cross-version and downstream evidence: the focused path/config
  group passed in isolated local environments on Python 3.11, 3.12, 3.13, and
  3.14. Weft callsites continue to use public `BrokerTarget` / `Queue` and do
  not consume the removed warning. The required local-checkout integration
  command passed. Weft already had unrelated modifications in
  `weft/commands/queue.py`, `weft/core/tasks/consumer.py`, and
  `weft/core/tasks/multiqueue_watcher.py`; none was edited.
- 2026-08-23 pre-review full gates: `uv run pytest -q`, full Ruff check and
  format check, full configured mypy command, DOM-15 fixtures, plan context,
  doc paths, coalescing evidence, and `git diff --check` passed. Interface
  review closed IR-1 and IR-2 with no runbook change recommended. The plan
  remains active and uncommitted pending independent completed-work review and
  the owner-controlled landing commit.
- 2026-08-23 Task 5 traceability reconciliation: final implementation/test
  mappings were added to the winning specs. The resulting spec diff against
  promotion baseline `d63e655` has SHA-256
  `03a5158b150ccec31392d07e2b129d0539a892cc7d64efee4da2654f3dfb2c97`.
- 2026-08-23 Task 6 current-tree gates and final review: added a direct compact
  loop firing case, then reran full pytest, Ruff check/format, configured mypy,
  all document/context/coalescing/diff gates, and the isolated focused Python
  3.11–3.14 matrix; all passed. A fresh-context GPT-5.6 completed-work reviewer
  returned PASS under DOM-11 with no blocking findings. The only residual risk
  is the ratified, documented TOCTOU boundary in an attacker-writable directory.
  The TDD, codebase-design, interface-review, testing, hardening, and
  adversarial-probe guidance needed no change: the reusable trust-boundary rule
  already exists as interface-review principle 10, while the product-specific
  behavior now belongs to the winning specs and configuration guide rather
  than a duplicate lesson. The owner then authorized a targeted landing commit;
  this plan and its Status Index row close in that same change.

## Review Log

| Date | Reviewer | Scope and baseline | Finding | Disposition |
|------|----------|--------------------|---------|-------------|
| 2026-08-23 | fresh-context GPT-5.5 plan reviewer | Full Class-5 plan, exact Strategy-A delta, current runtime/tests, overlapping maintainability work at `32210e58` | BLOCKED: status/vacuum open before the proposed preparation seam; dangling-symlink wording ambiguous; interface line pins will move | Accepted. Added status/vacuum to the shared prepared-target order and tests; kept init/cleanup separate; made dangling-final compatibility precise; required line-evidence refresh after rebase. Scoped re-review requested. |
| 2026-08-23 | same fresh-context GPT-5.5 reviewer | Scoped re-review of the accepted corrections | PASS: status/vacuum ordering, exact delta, tests, out-of-scope boundary, dangling-final semantics, and line-refresh gate are now coherent | Accepted; no remaining plan-review blocker. Implementation remains gated on the overlapping plan's landing baseline and rebase. |
| 2026-08-23 | fresh-context GPT-5.5 runtime-slice reviewer | Uncommitted runtime/tests after Tasks 2–4, promoted specs, program theory, and focused live probes | PASS: no relative command/status/vacuum path bypasses shared preparation; canonical target identity, non-strict dangling-final support, fail-closed loop/depth/read behavior, absolute/project exemptions, and warning removal are coherent. Non-blocking notes: reconcile the then-stale plan/index evidence before docs integration. | Accepted. Runtime design retained; plan/index, traceability, full gates, cross-version, and Weft evidence were reconciled before final completed-work review. |
| 2026-08-23 | fresh-context GPT-5.6 completed-work reviewer | Full uncommitted diff against `d63e655`, final specs/digest, runtime/tests, guidance/changelog, current-tree gates, Python matrix, and Weft evidence | PASS under DOM-11 with no blocking findings. Implementation matches `[SB-CLI-2]` / `[SB-API-2]`; compact has direct firing evidence; public shape and exemptions are preserved; interface review is credible. Residual risk is the intentionally excluded hostile-directory TOCTOU case. | Accepted. No code or contract revision required. The owner subsequently authorized a targeted landing commit, so the plan/index close in that same change. |
