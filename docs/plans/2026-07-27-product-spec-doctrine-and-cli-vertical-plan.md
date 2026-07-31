# Product Spec Doctrine and CLI Vertical Plan

**Date:** 2026-07-27  
**Status:** completed  
**Class:** **5+P** — normative process/product documentation doctrine changes
and creation of product-spec surface; process-material.  
**Hardening:** **required** — contract ownership, migration order, drift, and
rollback are one-way for agents and reviewers if wrong. Checklist in §9.  
**Owner:** repository maintainers under explicit authorization.  
**Decision prerequisite:**
`docs/plans/2026-07-27-product-docs-source-ownership-decision.md`  
**Completed:** 2026-07-28 — promotion slice §4.1–§4.10 landed; outside review
passed before implementation.

## 1. Goal

In **one coherent, completable unit of work**:

1. Promote **layered product-docs ownership** and the
   `readme-only → draft-spec → canonical-spec` registry into the repo’s
   process docs (exact text below).
2. Create the **product section registry** and the first product spec file.
3. Promote **one vertical** — **CLI contract `[SB-CLI-1]`…`[SB-CLI-4]`** — to
   **`canonical-spec` with every clause fully bound** (obligation →
   implementation mapping → firing test). No half-bound pilot.
4. Produce a **bounded invariant inventory** (find/map only) for the next
   verticals — **no** second promotion in this plan.

**Out of this plan:** Backstitch install/CI (follow-on plan only, after this
shape is proven), hosted docs host choice, full README progressive-disclosure
rewrite, embedding specs, multi-family promotions, permanent “IA maintenance
program.”

## 2. Spec baseline

| Item | Value |
|------|--------|
| **Spec / doctrine baseline SHA** | `5c67631` (`docs: add agent use kernel and llms.txt index`) |
| **Product behavior baseline** | Root `README.md` at `5c67631` (unchanged product semantics in this plan) |
| **Promotion strategy** | **B — atomic multi-file**: doctrine files + registry + first product spec + README/kernel pointer updates + tests in **one** promotion slice after review of this plan’s delta |
| **What must not change** | CLI exit-code values, meanings, or runtime behavior; package public API |

If `main` has moved past `5c67631` before implementation, re-diff the touched
paths against the new tip, update this plan’s baseline identifier, and
re-check the proposed text still applies (no silent baseline drift).

## 3. Source documents

- Decision: `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`
- Doctrine today: `docs/README.md`, `docs/specs/README.md`,
  `docs/specs/00-specs-index.md` @ `5c67631`
- Product CLI contract today: root `README.md` § Exit Codes @ `5c67631`
- Agent view: `docs/agent-kernel.md` § Exit codes and I/O (CLI)
- Gates today: `tests/test_documented_exit_codes.py`,
  `tests/test_agent_kernel_contract.py`
- Constants: `simplebroker/_constants.py` (`EXIT_SUCCESS`, `EXIT_ERROR`,
  `EXIT_QUEUE_EMPTY`)
- Superseded roadmap (do not execute):
  `docs/plans/2026-07-27-information-architecture-improvement-plan.md`

## 4. Proposed Spec Delta (exact)

### 4.1 Replace `docs/README.md` body

**Entire file becomes:**

```markdown
# SimpleBroker Docs

Design, planning, and specification record for SimpleBroker.

## Product documentation ownership

Product behavior is governed by a **layered source-of-truth system** (decision:
`docs/plans/2026-07-27-product-docs-source-ownership-decision.md`):

| Surface | Role |
|---------|------|
| Root `README.md` | Human entry, command/env catalogs, and **normative text for every product concern still in state `readme-only`** |
| `docs/specs/` product sections (`[SB-*]`) | **Normative** when the product section registry marks the section `canonical-spec` |
| `CHANGELOG.md` | Behavior deltas for published releases |
| `docs/agent-kernel.md` | Agent-oriented **view** of use-level rules; must not invent obligations beyond the winning SoT |
| `llms.txt` | Machine-readable link index (not normative) |

**Conflict rule:** If a product section is `canonical-spec` in
`docs/specs/product-section-registry.md`, that spec section wins over
README prose. README may restate short tables and must link the section
code. If the section is `readme-only` or `draft-spec`, root `README.md`
wins.

**Migration states** (see registry and ownership decision): `readme-only` →
`draft-spec` → `canonical-spec` for extracting concerns from the README.
Promotion to `canonical-spec` requires a gate for **every** numbered clause.
Once canonical, **update the product spec in place** ([DOM-6] /
`writing-specs.md`); do not retire or de-promote product sections the way
plans are coalesced. Unreleased promotion mistakes: git revert. Migration
transitions are atomic (registry + spec + README pointer + gates).

## Process and agent guidance

As of 2026-07-16 this repository carries the agent-guidance operating
model (adopted from agent-guidance @ `fc23eae`): shared agent context in
`docs/agent-context/` (start at its README for the read order), process
specs in `docs/specs/` (`[DOM-*]`), implementation docs in
`docs/implementation/`, reusable skills in `skills/`, the lessons ledger
in `docs/lessons.md`, and coalescing state in `docs/coalescing.md`.
Agents start at `AGENTS.md`. For product *use* orientation, prefer
`docs/agent-kernel.md` before deep-diving the root README.

Plans in `docs/plans/` are dated implementation plans and historical
execution references.

These docs do not contain plans prior to **2026-03-16**, when formal on-disk
planning started. Design decisions made before that date live only in commit
messages and the CHANGELOG. Plans with dated filenames (`YYYY-MM-DD-...`)
follow the naming convention adopted 2026-04-02; the handful of undated plans
predate it.
```

### 4.2 Replace `docs/specs/README.md` body

**Entire file becomes:**

```markdown
# Specs

See `00-specs-index.md` for the canonical numbered entry point and reading
order for this directory.

## Scope (this repository)

This tree holds:

1. **Process specs** — development operating model (`[DOM-*]`), still
   governing how work is planned, reviewed, and documented.
2. **Product specs** — intended product behavior under stable `[SB-*]`
   codes, when present.

Product section **authority** is not “every file in this directory wins.”
Authority is defined by `product-section-registry.md`:

- `readme-only` — no canonical product section yet; root `README.md` is
  normative for that concern.
- `draft-spec` — scaffold under review; root `README.md` still wins on
  conflict.
- `canonical-spec` — the listed spec section is normative; README restates
  and links.

`CHANGELOG.md` records published behavior deltas. `docs/agent-kernel.md`
is a non-authoritative agent view.

Nothing in a `draft-spec` or unregistered file supersedes the root README.
```

### 4.3 Replace `docs/specs/00-specs-index.md` body

**Entire file becomes:**

```markdown
# Specs Index

Numbered entry point for `docs/specs/`. Directory `README.md` is a thin
pointer; this file is the read order.

## Process specs

1. `01-development-documentation-operating-model.md` — `[DOM-*]`

## Product specs

Product section authority: `product-section-registry.md`.

1. `10-cli.md` — `[SB-CLI-*]` (exit codes and CLI I/O when
   registry marks canonical)

## Rules

- Specs define intended behavior, invariants, and verification expectations.
- Specs use stable reference codes so plans and code can cite exact
  requirements.
- Specs backlink related plans under `## Related Plans`.
- If behavior changes materially, update the **winning** SoT (registry
  state) before or with the code, and record user-visible deltas in
  `CHANGELOG.md` when applicable.
- Do not treat `draft-spec` product files as canonical.

## Naming

- Process: keep `01-…` and `[DOM-*]`.
- Product: `10+` filenames and `[SB-*]` codes.
- Prefer concise, descriptive titles over ticket-like names.

## Related Surfaces

- Root `README.md` — human entry and `readme-only` / `draft-spec` residual
- `docs/agent-kernel.md` — agent use orientation
- `docs/plans/` — execution
- `docs/implementation/` — rationale and repository maps
- `skills/` — reusable workflow instructions
- `product-section-registry.md` — mechanical ownership registry
```

### 4.4 New file `docs/specs/product-section-registry.md`

**Full file content:**

```markdown
# Product Section Registry

Mechanical authority table for product documentation. **One row per
concern family.** States: `readme-only` | `draft-spec` | `canonical-spec`
(see `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`).

| Concern | State | Spec section | README anchor / locus | Gate (obligation → impl → test) |
|---------|-------|--------------|----------------------|----------------------------------|
| CLI exit codes and CLI I/O contract | `canonical-spec` | `10-cli.md` `[SB-CLI-1]`…`[SB-CLI-4]` | `### Exit Codes` (+ kernel Exit codes) | `tests/test_documented_exit_codes.py` (SB-CLI-1 + README link); `tests/test_agent_kernel_contract.py` (SB-CLI-1 + kernel link); `tests/test_cli_contract_sb_cli.py` (SB-CLI-2, SB-CLI-3, SB-CLI-4 behavioral binds) |
| Delivery guarantees, claim/peek/watch safety | `readme-only` | — | README Critical Safety / Delivery; agent-kernel Delivery | (future) |
| Message identity (hybrid ts, last_ts, move+checkpoint) | `readme-only` | — | README Core Concepts / agent-kernel Message IDs | (future) |
| Dump/load and claimed-row I/O | `readme-only` | — | README dump/load | (future) |
| Embedding targets, backends, sidecar | `readme-only` | — | README Embedding / Advanced | (future) |

## Transition rule

A **migration** state change requires one PR that updates this table, the
spec file (if any), the README pointer when entering `canonical-spec`, and
every Gate cell named for that row. **Entering `canonical-spec` requires a
firing test per numbered clause** (no unbound obligations). After
canonical, **edit the spec in place** for behavior/wording changes; update
this registry only when ownership or gates change (e.g. new clause + new
gate), not to “retire” the section. Incomplete migration transitions are
forbidden. Abandoning an **unshipped** `draft-spec` may return to
`readme-only` per the ownership decision.

## Related Plans

- `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`
- `docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
```

*(Implementer: on first land, the CLI row is written as `canonical-spec`
together with §4.5 in the same PR. Do not land registry with CLI as
`draft-spec` then flip in a second un-reviewed step inside this unit.)*

### 4.5 New file `docs/specs/10-cli.md`

**Full file content** (all four clauses are **fully bound** — each has
implementation mapping + firing test in §4.9; no half-bound pilot):

```markdown
# CLI Contract

Normative CLI process exit codes and byte-stream roles for the `broker` /
`simplebroker` entry points. Library `Queue` APIs use return values and
exceptions instead of these exit codes (see `docs/agent-kernel.md`).

## Exit code set [SB-CLI-1]

The CLI uses exactly three process exit codes:

| Code | Constant | Meaning |
|------|----------|---------|
| `0` | `EXIT_SUCCESS` | Success |
| `1` | `EXIT_ERROR` | Error |
| `2` | `EXIT_QUEUE_EMPTY` | Queue empty / nothing to do (not a crash) |

No additional exit codes may be introduced without updating this section,
the root README Exit Codes list, `simplebroker/_constants.py`, and the
exit-code gates.

_Implementation mapping_:
- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`

## Stdout and stderr [SB-CLI-2]

- **stdout** carries command data (messages, JSON records, dumps).
- **stderr** carries diagnostics, warnings, and human progress noise.
- On a successful data-bearing read that prints a message body (plain or
  JSON), the message payload appears on **stdout**, not only on stderr.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`

## Global options position [SB-CLI-3]

Global options (for example `-f` / `--file`, `-d` / `--dir`) must appear
**before** the subcommand. Placing them after the subcommand is not
supported as an alternate grammar: the process exits `1` (`EXIT_ERROR`)
with an argument-parse failure (for example unrecognized arguments).

_Implementation mapping_:
- `simplebroker/cli.py`

## Message-line JSON fields [SB-CLI-4]

**Scope:** JSON (or NDJSON) **message lines** emitted by queue data commands
that print message bodies with ids — specifically **`read`**, **`peek`**,
**`move`**, and **`dump`** when those commands use `--json` (or dump's
JSON line format). This clause does **not** apply to other `--json`
shapes (for example `list --json` emits `{"queue": ...}` objects without
`message`/`timestamp`).

Each message-line object includes at least:

- `message` — message body string
- `timestamp` — message id (hybrid timestamp integer)

_Implementation mapping_:
- `simplebroker/commands.py` (message JSON emission helpers)

## Related Plans

- `docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`

## Verification

- `tests/test_documented_exit_codes.py` — [SB-CLI-1] + README link
- `tests/test_agent_kernel_contract.py` — [SB-CLI-1] + kernel link
- `tests/test_cli_contract_sb_cli.py` — [SB-CLI-2], [SB-CLI-3], [SB-CLI-4]
```

### 4.6 Root `README.md` — Exit Codes section (exact insertion policy)

**Do not rewrite the whole README in this plan.**

In `### Exit Codes` (root README), **immediately after** the existing three
bullet list of `` `0` `` / `` `1` `` / `` `2` `` (keep the bullets; they stay
as the human restatement), insert exactly:

```markdown

Normative detail: `docs/specs/10-cli.md` ([SB-CLI-1]–[SB-CLI-4]).
```

If that subsection’s bullet list is reordered later, the sentence must remain
in the Exit Codes subsection and the registry README locus must be updated in
the same change.

### 4.7 `docs/agent-kernel.md` — Exit codes subsection

After the exit-code table in `## Exit codes and I/O (CLI)`, insert exactly:

```markdown

Normative: `docs/specs/10-cli.md` [SB-CLI-1]–[SB-CLI-4].
```

### 4.8 `llms.txt`

Under `## Docs`, **after** the Agent kernel bullet, insert exactly:

```markdown
- [Product section registry](docs/specs/product-section-registry.md): Mechanical ownership state for product doc sections (`readme-only` / `draft-spec` / `canonical-spec`)
- [CLI contract spec](docs/specs/10-cli.md): Canonical CLI exit codes and stdout/stderr roles `[SB-CLI-*]`
- [Specs index](docs/specs/00-specs-index.md): Process and product spec read order
```

### 4.9 Tests — full bind for [SB-CLI-1]…[SB-CLI-4]

**Backstitch-readiness rule for this pilot:** every `canonical-spec` clause
must present obligation → implementation path → firing test. Do **not**
leave SB-CLI-2/3/4 as unbound text (that is the failure mode a later
`backstitch` `fail_on` would flag). Prefer a fully-bound pilot over a
half-bound one.

#### `tests/test_documented_exit_codes.py` (extend)

1. Parse `docs/specs/10-cli.md` `[SB-CLI-1]` table codes `{0,1,2}`
   and assert equality with
   `{EXIT_SUCCESS, EXIT_ERROR, EXIT_QUEUE_EMPTY}`.
2. Assert root README Exit Codes subsection contains
   `docs/specs/10-cli.md`.
3. Keep existing README bullet enumeration test.

#### `tests/test_agent_kernel_contract.py` (extend)

1. Assert kernel contains `docs/specs/10-cli.md` or `[SB-CLI-1]`.

#### New `tests/test_cli_contract_sb_cli.py` (behavioral binds)

Use real CLI invocation (existing test helpers / subprocess patterns; no
mock of argparse). Pin an explicit temp database with **global** `-f`
before the subcommand.

| Clause | Required test behavior (holds on current main; verified 2026-07-27) |
|--------|---------------------------------------------------------------------|
| **[SB-CLI-2]** | `broker -f DB write q hi` then `broker -f DB read q` → rc=0, body on **stdout** (not only stderr). Optional: `read --json` body on stdout. |
| **[SB-CLI-3]** | `broker read q -f DB` (global **after** subcommand) → rc=1 (`EXIT_ERROR`), parse failure / unrecognized arguments; does **not** succeed as an alternate grammar. Control: `broker -f DB read q` after a write succeeds with rc=0 or empty-appropriate codes as applicable. |
| **[SB-CLI-4]** | After write: `broker -f DB peek q --json` (and at least one of `read --json` / `move --json` to a dest) → stdout line(s) parse as JSON objects each containing keys `message` and `timestamp`. **Negative scope:** `broker -f DB list --json` may emit `{"queue": ...}` and must **not** be required to carry `message`/`timestamp` (clause explicitly excludes non-message JSON). |

No intentional runtime CLI behavior changes — these tests characterize
existing contracts so the pilot section is fully bound for a later
Backstitch plan.

### 4.9a Sequencing note (Backstitch)

**This plan does not install or run Backstitch.** Mental model:

1. **This unit** delivers bindable `[SB-*]` shape + IA doctrine + one fully
   gated vertical (obligation, mapping, tests).
2. **Next plan** (deferred): pin `backstitch`, advisory `check`, then scoped
   `fail_on` against this pilot — gated on this plan proving the section
   shape.

If implementers expected Backstitch green at the end of *this* unit, that is
a **scope mismatch**: stop and keep Backstitch in the deferred table only.

### 4.10 Inventory artifact (find/map only)

Create `docs/implementation/04-product-invariant-inventory.md` with a table
of candidate concerns for **later** plans (delivery, identity, dump/load,
embedding). Each row: claim summary, README/kernel locus, proposed future
`SB-*` family, known tests, state `readme-only`. **No** second
`canonical-spec` row in this plan.

Minimum rows (copy from decision/agent-kernel priorities):

| Family | Locus | Proposed codes | Notes |
|--------|-------|----------------|-------|
| Delivery / claim / peek-stream | README Critical Safety; agent-kernel Delivery | SB-DELIVERY-* | Next vertical candidate |
| Message identity / move+checkpoint | README Core Concepts; agent-kernel Message IDs | SB-ID-* | |
| Dump/load / claimed | README dump/load; agent-kernel Dump/load | SB-IO-* | |
| Embedding targets/backends | README Advanced | SB-EMBED-* | Separate program |

## 5. Implementation tasks (dependency order)

1. **Review gate:** independent plan review of this delta (Class 5); dispose
   findings in § Review Log.
2. **Spec-promotion slice (atomic strategy B):** apply §4.1–§4.10 in one PR
   (or one tightly stacked pair: docs+tests only). No product code changes.
3. **Verification:** §8 commands.
4. **Index:** mark this plan `completed` with evidence SHA; mark decision
   plan `active`/`completed` as accepted.
5. **Supersede roadmap:** set
   `2026-07-27-information-architecture-improvement-plan.md` index status to
   `superseded` by this plan + decision (roadmap not executable).

## 6. Explicitly deferred (own future plans)

| Program | Trigger to start |
|---------|------------------|
| Backstitch adoption | **After this plan completes** and proves fully-bound `[SB-CLI-*]` shape; separate plan: pin version → advisory `check` → optional scoped `fail_on`. Not part of this unit's DoD. |
| Hosted docs (one of RTD **or** GH Pages) | After choosing **one** primary host in that plan’s decision section |
| README progressive disclosure / collapse | After ≥1 more `canonical-spec` families **or** explicit density-only decision with no further promotions |
| SB-DELIVERY / SB-ID / SB-IO promotions | Separate Class 5 plans citing this registry machine |

## 7. Invariants (this unit)

1. Exit codes remain `{0,1,2}` with existing meanings.
2. No new CLI flags, commands, or package exports.
3. Registry has exactly one authority state per concern row.
4. `draft-spec` never wins over README.
5. Agent-kernel and llms.txt do not become normative SoT.
6. Roadmap plan is not executed as a multi-phase program under this unit.

## 8. Verification and gates

```bash
# From repository root after the promotion slice
python3 bin/check-dom15-fixtures
uv run pytest -n0 \
  tests/test_documented_exit_codes.py \
  tests/test_agent_kernel_contract.py \
  tests/test_cli_contract_sb_cli.py \
  -q
# Optional smoke if desired:
# uv run pytest -n0 tests/test_smoke.py -q
git diff --check
```

Acceptance:

- [x] Files in §4 match proposed content (reviewer can diff against this plan)
- [x] Registry CLI row is `canonical-spec` with gates green
- [x] **All four** `[SB-CLI-1]`…`[SB-CLI-4]` have firing tests (no unbound
      canonical clauses)
- [x] SB-CLI-4 tests cover message-line JSON only; `list --json` is not
      required to carry `message`/`timestamp`
- [x] README Exit Codes links to `10-cli.md`
- [x] Kernel cites SB-CLI
- [x] llms.txt lists registry + CLI contract + specs index
- [x] Inventory file exists with ≥4 future families still `readme-only`
- [x] No Backstitch/MkDocs/RTD files added in this unit
- [x] Ownership decision: migration states + in-place update of canonical specs (no product-spec retirement)
- [x] Information-architecture roadmap plan marked superseded in index

## 9. Hardening checklist ([DOM-5] / hardening-plans)

| Item | This unit |
|------|-----------|
| What must not change | Exit codes, runtime CLI/library behavior, public exports |
| Edit points | Exact files in §4; constants only if test imports already cover them (no constant edits expected) |
| Real tests | Exit-code and kernel contract tests; no mocks of docs parsers beyond reading files |
| Rollback (unreleased promotion) | Git revert of the promotion PR is OK before a release advertises the section |
| Fix after ship | **Update the canonical product spec in place** (Class 5 as required); sync README restatement/links, gates, CHANGELOG — do not de-promote back to README as the primary path |
| Drift prevention | Registry + **per-clause** tests; conflict rule in docs/README; fully-bound pilot for backstitch follow-on |

| Stop gates | If README Exit Codes section cannot host the link without a larger rewrite, stop and amend this plan with exact alternate insertion text |
| One-way doors | Doctrine change is social/process one-way for agents; git revert remains possible |
| Downstream | Weft/Taut: docs-only; no pin bump |

## 10. Independent review

- **Before implementation:** different agent family reviews this plan’s §4
  exact text against tree at baseline `5c67631` (or updated baseline).
- Prompt: Can you apply §4 as a patch without guessing? Does the registry
  prevent dual-truth? Is scope free of Backstitch/hosted docs?

## 11. Out of scope

- Installing `backstitch` or adding CI jobs for it  
- MkDocs, Sphinx, Read the Docs, GitHub Pages  
- Collapsing the full root README  
- Promoting delivery/identity/dump/load/embedding sections  
- Cross-thread / poison generator work  

## 12. Success criteria

- Doctrine and registry are live and mechanical.
- Exactly one product vertical (`SB-CLI-*`) is `canonical-spec` with gates.
- Next work is clearly **other plans**, not phases of a roadmap.

## Task checklist

- [x] Independent plan review disposed
- [x] Spec-promotion slice §4.1–§4.10 landed
- [x] Verification commands green
- [x] Roadmap IA plan superseded in status index
- [x] This plan + decision index rows updated to completed

## Deviation Log

| Spec / decision | Planned | Actual | Rationale |
|-----------------|---------|--------|-----------|
| §4.10 inventory path | `docs/implementation/04-product-invariant-inventory.md` | `docs/implementation/05-product-invariant-inventory.md` | `04-cross-thread-finalization-poisoning.md` already occupied the `04-` slot |
| Alignment docs (beyond §4 exact list) | Not listed | Minimal ownership rewrites in `AGENTS.md`, `docs/implementation/01-…`, `02-…`, `00-implementation-index.md` | Prevent agents following stale “README-only product SoT” after doctrine land |

## Review Log

| Date | Reviewer | Verdict | Dispositions |
|------|----------|---------|--------------|
| 2026-07-28 | Outside review (user-reported) | pass | Proceed to implement; no open findings |

## Related Plans

- Decision: `2026-07-27-product-docs-source-ownership-decision.md`
- Supersedes (as executable roadmap):
  `2026-07-27-information-architecture-improvement-plan.md`
