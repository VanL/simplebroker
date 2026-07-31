# Information Architecture Improvement Plan

**Date:** 2026-07-27  
**Status:** superseded — roadmap only; **do not execute**  
**Superseded by:**
`docs/plans/2026-07-27-product-docs-source-ownership-decision.md` and
`docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
(engineering review: Class 5+P required, exact delta missing, excess scope).  
**Class:** n/a (not executable)  
**Owner:** repository maintainers / agents under explicit authorization.  
**Product contract:** historical notes only; see successor plans for doctrine.

## 1. Goal (historical roadmap — not a task list)

Improve SimpleBroker’s documentation architecture so that:

1. **Humans** get progressive disclosure (orientation → use → advanced)
   without losing a navigable product contract.
2. **Agents** get a stable path: `llms.txt` → `docs/agent-kernel.md` →
   specs/README, without a third free-floating SoT.
3. **Invariants** that currently live only as README prose become
   **findable, coded, and mappable** to owners and tests.
4. **Backstitch** (`../backstitch`, PyPI `backstitch`) is adopted
   **incrementally** — deterministic trace first, semantic lane later or never
   by default.
5. **Hosted docs** (Read the Docs and/or GitHub Pages) publish a
   markdown-native site compatible with the same tree agents edit in-repo.

Non-goals for this plan as a whole: rewriting product behavior; bulk-deleting
the README; full semantic backstitch in CI; migrating every README section in
one PR.

## 2. Problem Statement

| Pressure | Current state |
|----------|----------------|
| Density feedback | Root README ~2400 lines; single altitude for orientation and edge cases |
| Agent-context model | Specs define intended behavior with stable codes; process specs only today |
| Product SoT doctrine | `docs/README.md` / `docs/specs/README.md`: product = root README only |
| Traceability | Skills mention backstitch as optional gate; **no** `tool.backstitch` profile |
| Hosted docs | No MkDocs/Sphinx/RTD/GH Pages site; GitHub blob is the only published surface |
| Existing agent surfaces | `llms.txt` + `docs/agent-kernel.md` + contract tests — good, must stay aligned |

Tension: **single comprehensive README** vs **explicit invariants +
spec↔implementation mapping**. Resolution in this plan: **layered SoT** (one
system, multiple surfaces), not “delete the README” or “never leave the
README.”

## 3. Source Documents

| Document | Role |
|----------|------|
| Root `README.md` | Current product contract (behavior) |
| `docs/agent-kernel.md` | Agent use kernel (derived view) |
| `llms.txt` | llmstxt.org link index |
| `docs/README.md`, `docs/specs/README.md`, `docs/specs/00-specs-index.md` | Doctrine and process specs |
| `docs/agent-context/*` | Operating model (DOM, writing-specs, writing-implementation-docs) |
| `AGENTS.md` | Agent entry |
| `tests/test_agent_kernel_contract.py`, `tests/test_documented_exit_codes.py` | Existing doc gates |
| `../backstitch` README + specs | Trace lane, invariant binds, profile config |
| Hosted-docs ecosystem | MkDocs Material (preferred candidate), Sphinx, RTD, GH Pages |

**Spec baseline (process):** committed tree at plan authoring; product baseline
is root README at the same tip. Record SHAs in each promotion slice.

## 4. Target Architecture

```text
llms.txt                    machine link index (repo root; not necessarily in sdist)
README.md                   human entry: short orientation + catalogs + deep links
docs/agent-kernel.md        agent orientation (use first; embedding optional)
docs/specs/01-DOM-*.md      process (existing)
docs/specs/1x-SB-*.md       product invariants (new; stable [SB-*] codes)
docs/implementation/        why / maps / module ownership (extend beyond process)
docs/site/ or mkdocs.yml    published HTML (generated; source remains markdown in docs/)
code + tests                backlinks + invariant binds (phased)
CHANGELOG.md                deltas
```

**Ownership rule (doctrine to adopt in Phase A):**

- While a behavior has **no** product-spec section: root README remains
  normative for that behavior.
- Once a section is **migrated** and listed in the product specs index with
  status `canonical`: the **spec section wins**; README may restate short
  tables and **must** link to the code; drift is a gate failure.
- `docs/agent-kernel.md` is a **view** of use-level invariants, not a third
  SoT; it cites `[SB-*]` when codes exist.

## 5. Proposed Product Spec Inventory (initial)

Stable prefix: **`SB-`**. Files (names adjustable at Phase B):

| File (proposed) | Codes (examples) | Source of first draft |
|-----------------|------------------|------------------------|
| `docs/specs/10-product-core.md` | SB-CORE-* | README Features / Not for / Architecture pitch |
| `docs/specs/10-cli.md` | SB-CLI-* | Exit codes, stdout/stderr, global-before-command, json fields |
| `docs/specs/12-delivery.md` | SB-DELIVERY-* | Claim-before-process, generators, watch safety |
| `docs/specs/13-message-identity.md` | SB-ID-* | Hybrid timestamps, last_ts, move+checkpoint skip |
| `docs/specs/14-persistence-io.md` | SB-IO-* | Dump/load, claimed rows, vacuum, SQLite companions |
| `docs/specs/15-embedding.md` | SB-EMBED-* | Targets, backends, sidecar, multi-queue (later) |

Process DOM remains `01-…`. Numbering leaves room under `02–09` if needed.

Each product section minimum shape (writing-specs + backstitch-friendly):

```markdown
## Title [SB-EXAMPLE-1]

Normative statement…

_Implementation mapping_:
- `simplebroker/path.py`

## Related Plans
- …
```

Invariants that bind tests (backstitch form, when ready):

```markdown
### Invariant: no delete-while-peek-stream [SB-DELIVERY-PEEK-1]
…
```

(Exact backstitch invariant syntax follows installed `backstitch guide
alignment` at adoption time — do not invent a second grammar.)

## 6. Invariants: find / map / promote

### 6.1 Find (inventory)

Produce `docs/implementation/XX-product-invariant-inventory.md` (or a plan
appendix first) by harvesting:

| Source | What to extract |
|--------|-----------------|
| Root README Critical Safety, Delivery, Core Concepts, Architecture | Explicit “must / never / default” claims |
| `docs/agent-kernel.md` | Already-prioritized agent hazards |
| Lessons ledger / Golden Rules (product-relevant) | Recurring corrections |
| Characterization tests | e.g. move+checkpoint, exactly-once, path security |
| Weft/Taut integration lessons (optional cross-repo) | Embedder footguns (claim vs history) |

Each inventory row: **claim**, **source location**, **severity** (use /
embed / ops), **existing tests** (if any), **proposed code** (SB-*),
**migration status** (`readme-only` / `draft-spec` / `canonical` / `gated`).

### 6.2 Map

For each draft SB-* section:

1. Name **implementation owner path(s)** under `simplebroker/` (and
   extensions only when backend-specific).
2. Name **primary test module(s)** under `tests/` or extension tests.
3. Add reciprocal **backlink** in code docstrings when the section is
   promoted (Phase C+), not in the inventory-only phase.

### 6.3 Promote

Per section: inventory row → draft spec → README link + “canonical at SB-*”
→ optional invariant bind → gate. Prefer **small vertical slices** (one family
per PR) over bulk paste of the README.

**First verticals (ordered by agent failure cost):**

1. SB-CLI exit codes / I/O (already partially gated)  
2. SB-DELIVERY claim / peek-stream / move-reserve  
3. SB-ID last_ts / move+checkpoint  
4. SB-IO dump/load  

## 7. Delivery units / phases

### Phase A — Doctrine and IA skeleton (class 3 / 5 if normative)

**Outcomes:**

- Update `docs/README.md`, `docs/specs/README.md`, `docs/specs/00-specs-index.md`
  with **layered SoT** doctrine (README entry + residual; product specs
  canonical when marked; agent-kernel is a view).
- Add empty or stub product specs index section and placeholder file list
  (no behavior rewrite required).
- README top: short **orientation + stop-here for simple use** pointer;
  link `docs/agent-kernel.md` and hosted docs URL when live (placeholder OK).
- Repository map: document the layered tree.

**Verify:** human review of doctrine; `python3 bin/check-dom15-fixtures`; no
runtime tests required.

### Phase B — Invariant inventory (class 2–3)

**Outcomes:**

- Full inventory doc (§6.1) with proposed SB-* codes and test mappings.
- Independent sample review of ≥10 high-severity rows for false “already
  shipped / already tested” claims.

**Verify:** inventory completeness checklist against README TOC + agent-kernel
sections; no silent invention of completed mappings.

### Phase C — First product-spec promotions (class 5 per slice)

**Outcomes per slice:**

- Normative SB-* sections with `_Implementation mapping_`.
- README: restatement table or short bullets + link to codes; remove or
  collapse duplicated long prose once linked.
- `docs/agent-kernel.md`: cite codes; keep recipes.
- Minimal code backlinks on owner modules when backstitch is ready (or
  interim comment form matching future backstitch grammar).

**Verify:** existing behavior tests still pass; new/extended doc gates for
promoted enumerables; agent-kernel contract tests green.

### Phase D — Backstitch incremental adoption (class 3–4)

**Principle:** start **narrow, advisory, deterministic**; expand `fail_on`
only after noise is understood.

| Step | Action | Gate posture |
|------|--------|--------------|
| D0 | Document decision: pin `backstitch` version (PyPI or path dep for monorepo dev); note `../backstitch` as sibling reference | — |
| D1 | Add `[tool.backstitch.profile]` (or project config file per installed version): `spec_roots = ["docs/specs"]`, `code_roots = ["simplebroker", "tests"]` (and later `extensions/...` if desired), `plan_roots = ["docs/plans"]`, `test_roots = ["tests"]` | local only |
| D2 | Run `backstitch check` / `obligation list` against **process specs only** (DOM) + any product stubs; fix or suppress with **audited** reasons | advisory CI job optional |
| D3 | After first SB-* sections land: require mappings + backlinks for **those files only** (path selectors / policy) | warn → then fail for selected codes |
| D4 | Bind **draft** then **required** invariants for SB-DELIVERY / SB-ID families to real tests | fail_on for required only |
| D5 | Optional: semantic lane (`packets` / `analyze`) — **advisory forever by default** unless a separate decision enables fail | not blocking in this plan’s baseline |

**Out of scope for D:** replacing pytest; forcing LLM credentials in CI;
scanning all historical plans as obligations.

**Verify:** `uv run backstitch check` (exact CLI per pinned version); CI job
documenting exit codes; policy file reviewed like any contract change.

**Rollback:** remove CI job and profile; specs remain useful without the tool.

### Phase E — Hosted docs (Read the Docs and/or GitHub Pages) (class 3–4)

**Preference:** **MkDocs + Material** (markdown-native, matches in-repo
`.md`, RTD and GH Pages both supported). Sphinx only if a later decision
requires autodoc-heavy API from docstrings as the primary site.

**Outcomes:**

| Item | Notes |
|------|--------|
| `mkdocs.yml` (or `docs/` config) | Nav: Home (README extract or docs/index), Agent kernel, Specs, Implementation, Plans index (optional), Changelog |
| Build inputs | Prefer **same** markdown paths agents edit; avoid a parallel RST tree |
| RTD | `.readthedocs.yaml` + Python build; pin mkdocs plugins |
| GH Pages | Workflow: build on main, deploy `gh-pages` or Actions artifact pages |
| Dual host | Choose **one primary** public URL in README/llms; secondary optional |
| Versioning | Optional later (mike / RTD versions); not required for v1 site |
| sdist | Hosted site is **not** required inside the wheel; keep `llms.txt` /
  agent-kernel as repo/site artifacts |

**Content rules for the site:**

- Do not invent a second product contract on the site.
- Site nav should mirror progressive disclosure: Use → Specs → Advanced.
- Generated HTML must not become the edit surface.

**Verify:** `mkdocs build --strict` (or equivalent) in CI; link check for
internal nav; README badge/URL; llms.txt link to public HTML optional.

### Phase F — README progressive disclosure (class 3; may pair with C)

**Outcomes:**

- Top-of-README **orientation** (~1–2 screens): pitch, install, quick start,
  exit codes, Critical Safety summary (bullets + link), pointer to
  agent-kernel and hosted docs.
- Clear **“If you only need a queue, stop here / see Use”** boundary.
- Long embedding, env catalogs, architecture, reactor: **collapsed**
  (`<details>`) or linked to specs/implementation.
- Avoid deleting normative content until its SB-* home exists (Phase C).

**Verify:** manual readability pass; no removal of unmigrated normative
claims; agent-kernel + exit-code tests still pass.

### Phase G — llms.txt and agent-kernel maintenance (class 2–3, ongoing)

**Outcomes:**

- `llms.txt` remains llmstxt.org **link index**; update when new top-level
  surfaces land (product specs index, hosted docs URL, invariant inventory).
- `docs/agent-kernel.md` gains `[SB-*]` citations as codes appear; recipes
  stay executable.
- Extend `tests/test_agent_kernel_contract.py` (and siblings) when new
  enumerable kernel claims are added — **no holey gates**.
- Optional: gate that every `SB-*` code cited in agent-kernel exists in
  `docs/specs/`.

**Verify:** existing contract tests; link targets resolve; after RTD/Pages,
public URL listed in llms if desired.

## 8. Dependency order

```text
A doctrine ──► B inventory ──► C first promotions ──► D backstitch tighten
                 │                    │
                 └────► F README reshape (partial OK after A)
                 └────► E site skeleton (can parallel B; content after C)
                 └────► G llms/kernel continuous
```

Suggested first implementation PR: **A only**.  
Second: **B**.  
Third: **C1 (SB-CLI or SB-DELIVERY)** + **G**.  
Then **D1–D2**, **E skeleton**, **F** as README allows.

## 9. Invariants and constraints for this plan

1. **No silent dual SoT:** when a section is `canonical` in specs, README must
   not contradict it; prefer link over second full copy.
2. **No bulk README deletion** without a migrated home.
3. **Product behavior changes** still require README/CHANGELOG when user-facing
   — even after specs exist — until doctrine explicitly says otherwise for
   that surface.
4. **Backstitch fails closed only where configured;** default expand is warn.
5. **Semantic backstitch** is not a silent CI hard gate in this plan.
6. **Hosted docs** build from the same markdown; no “edit HTML.”
7. **Zero runtime deps** for the published package: docs tooling is
   dev/CI/docs-extra only.
8. **Weft** is primary downstream: product-spec promotions that restate
   public contracts should stay additive and visible in CHANGELOG if
   wording changes user obligations.

## 10. Testing and verification

| Area | Proof |
|------|--------|
| Doctrine | Diff review of docs/README + specs README + index |
| Inventory | Coverage of README TOC + agent-kernel sections |
| Promotions | Tests for each enumerable; backstitch check for mapped files |
| Agent kernel | `uv run pytest -n0 tests/test_agent_kernel_contract.py` |
| Exit codes | `tests/test_documented_exit_codes.py` |
| Backstitch | Pinned version `backstitch check` local + CI (posture per D) |
| Site | `mkdocs build --strict` (or chosen toolchain); optional linkchecker |
| Product | No unintended runtime change: `uv run pytest -n0 tests/test_smoke.py` when only docs |

## 11. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Spec/README drift | Canonical flag + gates; single ownership rule |
| Inventory false “shipped” | Sample independent review; evidence columns |
| Backstitch noise flood | Start DOM + few SB files; path-scoped fail_on |
| Site becomes stale | CI build on main; strict broken links |
| Agent-kernel becomes third SoT | Cite SB-*; contract tests; doctrine text |
| Scope explosion into product rewrite | Phase C one family per PR; no behavior change in A/B/E |

## 12. Out of scope

- Automatic healing / cross-thread generator product work (separate plan).
- Full migration of every README section in one change.
- Replacing AGENTS.md process model.
- Publishing agent-guidance hub content into the product site.
- Making `llms.txt` part of the PyPI wheel (optional later; not required).

## 13. Independent review

- **Plan review** before Phase A lands (different agent family).
- **Doctrine wording** review: is “canonical when marked” unambiguous?
- **After inventory:** sample review of high-severity SB-* proposals.
- **Before backstitch fail_on:** review policy selectors like a public API.
- **Before first public docs URL:** content + nav review against use/embedding
  split.

Review stance: Could a zero-context agent promote SB-DELIVERY-1 without
guessing ownership? Does the hybrid SoT avoid dual-truth?

## 14. Success criteria

- Layered SoT doctrine is written and linked from AGENTS / docs README.
- Invariant inventory exists with proposed codes and test maps.
- At least **one** product-spec family is `canonical` with mapping + gate.
- Backstitch runs deterministically in dev (and optionally CI) without
  blocking the whole tree incorrectly.
- Hosted docs build produces a navigable Use → Specs → Advanced site.
- `llms.txt` points at kernel, specs index, and public docs URL when live.
- Density: README orientation fits ~1–2 screens before deep contract.

## 15. Task checklist

- [ ] Phase A: doctrine + IA skeleton + README orientation pointers
- [ ] Phase B: product invariant inventory + proposed SB-* codes
- [ ] Phase C: first SB-* promotions (CLI / delivery / id / io as ordered)
- [ ] Phase D: backstitch profile → advisory check → scoped fail_on
- [ ] Phase E: MkDocs (or chosen) + RTD and/or GH Pages
- [ ] Phase F: README progressive disclosure (collapse advanced)
- [ ] Phase G: llms.txt + agent-kernel cite codes; extend contract tests
- [ ] Independent reviews at plan / inventory / fail_on / site milestones
- [ ] Index row for this plan: `draft` → `active` → `completed`

## 16. Fresh-eyes review prompt

> Read `docs/plans/2026-07-27-information-architecture-improvement-plan.md`,
> current `docs/README.md`, `docs/specs/README.md`, `docs/agent-kernel.md`,
> `llms.txt`, and the Backstitch quick start. Challenge: (1) Does layered SoT
> create dual-truth windows? (2) Is backstitch phasing fail-safe? (3) Is MkDocs
> the right default vs Sphinx for this repo? (4) Are first SB-* verticals the
> right failure-cost order?

## 17. Proposed Spec Delta

**Phase A (doctrine only)** — exact text drafted in the Phase A PR, not only
here:

- `docs/specs/README.md` and `docs/specs/00-specs-index.md`: product specs
  allowed; ownership rule for README vs canonical SB-* sections.
- `docs/README.md`: layered surfaces diagram; point at agent-kernel and
  future site.

**Later phases** promote product SB-* content under strategy A/B per
writing-plans (in-file or new numbered files). Each promotion slice records
its own baseline and promotion SHA.

## Deviation Log

| Spec / decision | Planned | Actual | Rationale |
|-----------------|---------|--------|-----------|
| (none yet) | | | |

## Review Log

| Date | Reviewer | Verdict | Dispositions |
|------|----------|---------|--------------|
| (pending) | | | |
