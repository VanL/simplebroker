# Host paths and database names

Status: completed
Class: 5 — owner-directed correction of public path admission revises
[SB-CLI-2] and [SB-API-2]. Hardening applies to the compatibility boundary.
Plan type: implementation with spec revision; promotion strategy A.
Owner: SimpleBroker core.
Boundary: name validation versus caller-selected filesystem ancestors.

## Goal

Accept host directories as supplied, including spaces and punctuation, without
applying database-name rules to ancestors outside the application's control.
Keep the database filename and optional project subdirectory in a compound
name restricted to ASCII letters, digits, dot, dash, and underscore, with the
existing 255-character component limit. No storage format changes.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-2], [THEORY-3]: a small local
  queue primitive with explicit backend ownership, usable by embedded callers.
- `docs/specs/10-cli.md` [SB-CLI-2/4]; `docs/specs/16-python-library-api.md`
  [SB-API-2]; `docs/specs/17-ops.md` [SB-OPS-7].
- `1c6898b:docs/plans/2026-09-13-verified-review-remediation-plan.md`, S5: the selected
  name grammar preserved generic ancestor checks under the old contract.
- `docs/implementation/07-complexity-and-state-machine-map.md`: path admission
  and target normalization are separate owners.
- `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`,
  `adversarial-acceptance-probes.md`, `review-loops-and-agent-bootstrap.md`,
  `designing-agent-facing-interfaces.md`; `skills/interface-review/SKILL.md`.
- Historical `c4819a7:simplebroker/_constants.py` and
  `c4819a7:simplebroker/_project_config.py`: whole-path restrictions predate S5.

## Spec Baseline

`4efe7b3` — specs 10 and 16, code, and prior remediation plan.
Owner direction on 2026-09-14 supersedes the retained ancestor restrictions.

## Context and Key Files

Modify `_constants.py` directory-field validators, `_project_config.py` resolved
SQLite target validation, and `cli.py` directory/absolute-file admission under
`simplebroker/`. Reuse `_validate_sqlite_filename`, `_db_name_path`, and the
existing physical containment resolver. Do not introduce a new validator.
Tests belong in `tests/test_path_security.py`, `tests/test_constants.py`, and
`tests/test_cli_validation.py` (correcting misleading missing-directory claims).
Update specs 10/16, configuration guide, implementation doc 07, CHANGELOG,
the dated lessons ledger, and the plan index.

The confirmed public failure is Weft's `resolve_context_broker_target` through
`target_for_directory` and `resolve_project_target` with `.weft/broker.toml`:
a leading/trailing-space ancestor is rejected although direct Queue access to
the same valid filename succeeds. CLI --dir/absolute --file and absolute
DEFAULT_DB_LOCATION/PROJECT_CONFIG_PATH reject the same ancestors. Ordinary
interior spaces already pass these paths and remain positive controls.

Comprehension gates (answer in execution log before code):
1. Which strings are compound names? Expected: DEFAULT_DB_NAME and relative
   --file; arbitrary Queue/project targets are filesystem paths, even relative.
2. Does accepting glob punctuation in ancestors widen owned-file cleanup?
   Expected: no; `_phaselock.py` calls `parent.glob(name + '.tmp.*')`, so the
   parent is literal and the database-derived pattern prefix remains restricted.
   Verify with a matching sibling directory and real cleanup, not a mock.

## Invariants and Constraints

- Keep raw and resolved terminal filename checks, including invalid symlink
  destinations, before database creation/mutation. Keep bounded diagnostics.
- Keep compound name grammar, 255-character maximum, one optional directory,
  traversal rejection, and relative-target physical containment.
- Host directory paths (CWD, --dir, absolute --file ancestors, directory fields,
  explicit API/project target ancestors) are not logical names. Do not trim,
  replace spaces, or apply component-name/length policy to them. Existing
  expanduser, resolve, existence/access checks and OS errors remain owned by
  path consumers; acceptance does not promise every path works on every OS.
- DEFAULT_DB_LOCATION stays absolute-or-empty; PROJECT_CONFIG_PATH stays
  absolute-or-one-relative-directory. Relative project-config prefix/name
  validation is unchanged; no new database grammar on TOML filenames.
- Explicit absolute CLI and trusted project targets retain containment
  exemptions. No target-precedence, error-code, remote-backend, or session change.
- No new dependencies, sidecar formats, background work, or release changes.
  Unexpected cleanup pattern interpretation or lost containment stops the slice
  for re-planning rather than weakening a test.

## Proposed Spec Delta

Strategy A: replace the two parent-path policy passages of [SB-CLI-2], and
insert the following boundary clarification in [SB-API-2] before its examples.

[SB-CLI-2], replace absolute-file sentence:

> For absolute `--file` paths, the grammar applies only to the terminal
> filename. The optional directory in a compound name is a project subdirectory
> (for example `.weft` in `.weft/broker.db`), not a restriction on the selected
> working directory or its ancestors.

[SB-CLI-2], replace paragraphs starting “Except for the SQLite filename grammar”
through the total-path ceiling:

> Host directory paths are accepted as supplied and are not subject to the
> database-name component grammar or its 255-character limit. This includes
> the working directory, `--dir`, `DEFAULT_DB_LOCATION`, absolute
> `PROJECT_CONFIG_PATH`, and ancestors of explicit CLI/API or project-config
> targets. Spaces (including leading or trailing spaces), Unicode, and
> punctuation in those directories are preserved. Existing path resolution,
> home expansion, existence/access checks, and operating-system limitations
> still apply; this does not relax relative-target physical containment.
> Name validation remains on the selected database filename and optional
> compound-name subdirectory. Relative project-config names and prefixes
> retain their separate validation rules.

[SB-API-2], insert before “Thus db_path” example:

> Arbitrary parent directories retain their spelling, including spaces and
> punctuation. Discovery and absolute directory configuration use the same
> host-path boundary; they do not revalidate resolved ancestors as names.

## Tasks

1. Independent plan/delta review, then spec promotion with backlink and
   promotion identifier. Correct false assumptions before production edits.
2. One linked implementation slice: remove generic full-path name validation
   from CLI directory/absolute-file routes, absolute configuration directory
   fields, and resolved project SQLite targets. Keep the existing name seams.
   First write failing public-boundary tests for interior/edge-space roots,
   ancestor punctuation and Unicode, with and without project TOML; explicit
   absolute/relative paths; absolute config fields; config/CLI invalid compound
   name controls. Exercise writes, reads/status and cleanup against real SQLite.
   Keep unrelated sibling files intact and assert exact selected target.
3. Independent implementation/interface review; align guide, implementation
   rationale, changelog, traceability and evidence. Run targeted tests and docs
   gates. User controls commit/publication; close index when closing is authorized.

## Verification

Do not mock filesystem paths, SQLite, CLI dispatch, config resolution or target
selection. POSIX-only spellings get explicit platform skips; ordinary spaces
remain cross-platform. Cover plain/JSON invalid-name exits, empty stdout, no
traceback, absence of mutation, invalid defaults, missing directories, and
existing containment tests. Batch-only and concurrent-processing probe floors
are not changed by this path-admission slice.

Commands: `uv run --locked pytest -o addopts= -q -n 0 tests/test_path_security.py
 tests/test_constants.py tests/test_cli_validation.py tests/test_config_builder.py
 tests/test_connection_config.py tests/test_project_config.py tests/test_cleanup.py`; scoped Ruff and
core mypy; `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
`bin/check-doc-paths`, `git diff --check`. Repeat Weft's public target-resolution
probe using its actual environment after the fix.

## Rollout and Rollback

No migration or ordering requirement. Observe successful write/read/cleanup in
a project rooted under spaces or literal punctuation, with invalid logical names
still rejected. Revert the admission patch to roll back; that restores the
ancestor rejection and can make newly admitted host paths unusable until their
caller upgrades again. No files are renamed and no publication is part of this
work. Existing filesystem permissions remain the trust boundary.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|

## Review Log

Independent plan/delta review passed. Reviewer verified the five boundary
removals, compound/raw/resolved-name invariants and actual literal-parent
cleanup with a matching sibling sentinel. Added tests/test_cleanup.py to the
verification cohort as requested.

Other-model review: read-only `claude -p <embedded-plan-and-scoped-diff>
--permission-mode plan --allowedTools Read,Grep,Glob`, 540-second bound,
exit 0 after 262.6 seconds. Verdict: no blocker. Reviewer verified all
remaining full-path validator callers are selected-name paths, raw/resolved
basename checks remain, containment remains, cleanup parents are literal,
and docs align. Its F1 was the same misleading missing-directory tests already
corrected and independently rerun above. Its F2 restated the intentional
trusted-project target containment exemption, not a defect or new capability.
Review output: /tmp/simplebroker-host-path-claude-review.txt.

## Execution Log

- Reproduced on baseline via CLI subprocess and Weft's real environment.
  Interior spaces pass; leading/trailing spaces fail only at full-path
  validation seams. Direct Queue write/read passes all these parent spellings.

- Comprehension answers: DEFAULT_DB_NAME and relative --file are compound-name
  selectors; explicit Queue/project targets are paths. The only runtime glob
  uses a literal parent and a database-derived basename pattern. SQLite cleanup
  uses os.scandir over the literal parent and exact basename prefixes. Existing
  expanduser semantics do not interpret a tilde embedded in an absolute ancestor.

- Spec promotion: [SB-CLI-2] and [SB-API-2] delta applied against 4efe7b3;
  promotion identity is that base plus this plan's exact Proposed Spec Delta
  and the spec diffs. Subsequent implementation is judged against this delta.

- Implementation: removed full-host-path name checks at the five reviewed
  boundaries; reused all existing raw/resolved filename, compound-name and
  containment owners. New public-path selection failed 11 cases and passed
  10 before the patch; the seven-module target cohort then passed 512 tests
  with 9 platform/filesystem skips. The separate symlink, CLI-dispatch, CLI
  contract and invalid-config lifecycle cohort passed 179 tests.
- Static gates: scoped Ruff/format checks and test mypy passed; core plus
  release-tool mypy passed for 46 files. Docs fixtures, plan context, doc paths
  and diff whitespace checks passed.
- Sol security review initially flagged control-character output. Baseline
  reproduction showed the same literal controls already emitted in rejection
  diagnostics at 4efe7b3, so the final disposition is no new security blocker.
  General control-safe diagnostic rendering is a separate pre-existing issue.
  The literal-parent cleanup proof preserved matching sibling and unrelated
  temp entries. Separate report: /tmp/simplebroker-host-path-security-sol.md.
- Interface skill/runbook evaluation: the current boundary, source-existence,
  real-sink and enumerated-error requirements covered this correction; no
  durable skill or runbook change is needed. The observed name-versus-host-path
  correction is recorded in the dated lessons ledger.

- Independent interface review confirmed clean missing-directory, missing-parent
  and invalid-filename errors in plain and JSON modes, preserving empty stdout
  and no mutation. Weft's actual interpreter against the candidate checkout
  resolved existing `.weft/broker.toml` targets under edge-space and literal
  wildcard ancestors. Its installed SimpleBroker remains the published version;
  this probe selected the candidate with PYTHONPATH without changing that install.
- Review feedback found two existing CLI tests that called nonexistent-path
  rejection character validation. Their names/assertions are corrected to prove
  the actual missing-directory error; new positive tests establish acceptance.

- Corrected CLI test descriptions/cause assertions passed their full module
  (18 passed, 2 Windows skips), Ruff/format, and the repository's test mypy mode.
  Independent interface review completed all eleven principles without a
  remaining blocker; report: /tmp/simplebroker-host-path-interface-review.md.
- Additional real CLI probe used relative --dir with edge spaces and the valid
  compound name `.weft/broker.db`: write/read succeeded and no trimmed sibling
  directory was created.

- Final evidence: 691 distinct targeted tests passed across the two cohorts,
  with 9 platform/filesystem skips; the subsequently corrected CLI module
  passed again. Scoped source/test static checks and documentation gates passed.
  No Windows runtime claim, package installation, publication, or storage
  migration is part of this change.

- Closure authorized with the 8.2.2 release request on 2026-09-14. The plan,
  contract, implementation, tests, guide, implementation rationale, changelog,
  and lesson are closed together in the targeted implementation commit.

- Release-preparation verification reran the seven-module behavior cohort:
  512 passed with 9 explicit platform/filesystem skips. Scoped Ruff, format,
  core mypy, DOM-15 fixtures, plan context, documentation paths, and whitespace
  checks passed. A fresh independent review found no blocker and reconfirmed
  the filename, containment, and literal-parent cleanup boundaries.
