# Docs Information Architecture Plan — Three-Purpose README and Guide Tier

Date: 2026-08-04
Status: active (index row is authoritative). Execution authorized by owner
2026-08-04 "implement per plan"; D1–D4 resolved to the plan's recommended
options.
Class: 4 per [DOM-15] — crosses multiple documentation surfaces, relocates
registered README loci, and retargets contract gates; no product-behavior
change and no spec-wording change.
Owner: SimpleBroker product owner (approval required before execution)

## Goal

Restructure the human documentation layer so the root `README.md` serves
exactly three purposes — (1) communicate the theory of the project to humans,
(2) provide rapid onboarding for the CLI and Python API, (3) route readers to
the owning document for everything else — while rehoming, not deleting, every
piece of essential information the README currently carries. Duplication is
resolved by rule: anything best described in a spec becomes a link to the
spec, not a reframing.

Target outcome: README shrinks from ~2,520 lines to ~600–700; a small
`docs/guides/` tier absorbs the advanced human material; `CONTRIBUTING.md`
absorbs the contributor/release material; no promise is added, removed,
narrowed, broadened, or reworded in the move.

## Guiding principle: excellence in OSS communication

The docs should read as examples of excellent open-source communication, not
merely as correct ones. Concrete models, chosen because each maps onto a
surface this plan touches:

- **Diátaxis** (<https://diataxis.fr/>) — the four-quadrant model
  (tutorial / how-to / reference / explanation) is the organizing skeleton.
  SimpleBroker already has three quadrants: Quick Start (tutorial-shaped),
  canonical specs (reference), program theory (explanation). The missing
  quadrant is **how-to** — that absence is why 1,400 lines of how-to material
  currently squat in the README. `docs/guides/` fills that quadrant.
- **SQLite** (<https://sqlite.org/whentouse.html>, "Appropriate Uses for
  SQLite") — the model for the README's "Recommended For / Not for" section:
  honest boundary-drawing that increases rather than decreases adoption
  trust. SimpleBroker's ownership-boundary paragraph is already in this
  spirit; the rewrite should sharpen it, not hedge it.
- **ripgrep** (`README.md` + `GUIDE.md` + `FAQ.md` split in
  BurntSushi/ripgrep) — the model for the mechanical split: a README that is
  pitch + install + a fast tour + a clearly signposted handoff to a
  long-form guide, with the guide owning depth.

Editorial bar for every touched page: lead with what the reader can do;
one idea per section; examples runnable as shown; no section that exists
because "the information had to go somewhere." When a section cannot say
what reader-task it serves, it moves or dies (dies = only for text that is
duplicative; promises never die, per the invariants).

### Editorial standard: Simplified Technical English

All new and rewritten prose in the touched docs uses Simplified Technical
English (ASD-STE100-inspired, adapted for software docs):

- Active voice. Present tense where possible.
- Short sentences: one instruction or one fact per sentence; target ≤ 20
  words for instructions, ≤ 25 for descriptions.
- One term per concept, used consistently: `queue`, `message`, `claim`,
  `broker target`, `backend` — never synonyms for the same thing. The
  glossary source is the program-theory concept table `[THEORY-3]`.
- Verb-first instructions ("Run `broker init`.", not "You will want to
  initialize…").
- No idioms, no filler ("simply", "just", "note that", "it should be
  noted").
- Warnings state the hazard, the consequence, and the required action, in
  that order.

**Carve-out (required by invariant 1):** sentences bound by contract gates
move verbatim, even where they violate STE. Rewording a bound promise into
STE is a contract-wording change: it needs an exact proposed delta,
explicit owner authorization, and a separate change per the registry
transition rule. Phase 0 marks each bound sentence so editors know which
text is frozen. Candidate STE rewrites of bound sentences may be collected
in an appendix for a later owner-authorized pass, but never applied in
this plan.

## Source documents

- `docs/README.md` — layered ownership declaration (roles table)
- `docs/specs/product-section-registry.md` — registry, README-locus column,
  TOC-ownership audit table, transition rule
- `docs/specs/00-specs-index.md` — read order and Related Surfaces
- `docs/program-theory.md` — `[THEORY-1/2/3/5]` feed the README theory
  section; `[DOM-16]` governs theory revisions (this plan makes none)
- `docs/agent-kernel.md` — agent orientation; its "Where to go next" table
  routes to README sections this plan moves
- `docs/specs/01-development-documentation-operating-model.md` — [DOM-5],
  [DOM-10], [DOM-11], [DOM-15], [DOM-16]
- `docs/agent-context/runbooks/hardening-plans.md` — checklist applied below
- Session analysis (2026-08-04): README section census, duplication
  inventory, gate-binding census (reproduced in Appendix A)

## Context and key files (current structure)

Sizes today: `README.md` 2,520 lines; `docs/agent-kernel.md` 368;
`docs/program-theory.md` 242; product specs `10-cli.md`…`17-ops.md` ≈ 1,290
total; `examples/README.md` 294.

The README currently interleaves five kinds of content:

1. **Orientation** — intro, Recommended For, Features, Use Cases, ownership
   boundary (~130 lines; Features/Use Cases largely duplicate Recommended
   For).
2. **Onboarding** — Install, Quick Start, basic Python API (~150 lines).
3. **Reference catalogs** — command tables, options, exit codes, env vars
   (~350 lines; the registry sanctions these as README-owned catalogs).
4. **How-to / advanced depth** — safety scripts, common patterns, watch
   modes, ~700 lines of advanced Python API (generators, cross-thread
   poisoning, sidecar, reactor, waiters, exact-ID insert), embedding,
   project scoping (~400 lines), backends, benchmarking (~1,500 lines
   total).
5. **Contributor/process** — development setup, lint/test commands, release
   procedure (~150 lines).

Known duplication instances (resolve per the rules in this plan):

- Inline `safe-worker.sh` + 65-line resilient-worker script vs
  `examples/resilient_worker.sh` vs kernel move-to-inflight recipe (kernel
  version is the best; keep it canonical).
- Broadcast semantics restated in README prose, kernel table, and
  `[SB-BCAST-*]`.
- Cross-thread finalization prose (~50 README lines) restating
  `[SB-DELIVERY-6]` and `docs/implementation/04-…`.
- Project-scoping precedence stated twice in one section (numbered list and
  flowchart); five near-identical "Common Use Cases" examples.

## Target architecture

| Purpose / Diátaxis quadrant | Surface | Content |
|---|---|---|
| Explanation (human theory) | README §Theory + `docs/program-theory.md` | README carries a ~80-line non-governing distillation of `[THEORY-1/2/5]` + the four design rules now in the Design Philosophy `<details>`; program theory remains the governed account |
| Tutorial (onboarding) | README §Install/Quick Start | CLI first; ~30-line matched `Queue` tour; one safety block |
| Reference (exact behavior) | `docs/specs/` + README catalogs | Unchanged specs; README keeps command/option/exit-code tables with `[SB-*]` links, stripped of deep-dive prose, plus a short "common settings" table (the ~6 most-used `BROKER_*` keys) linking the full catalog in `docs/guides/configuration.md` (final env locus — decided at plan time, review F5) |
| How-to (task depth) | **new `docs/guides/`** + `examples/` | Three guides (below); long shell patterns live as `examples/` files |
| Contributor/process | **new `CONTRIBUTING.md`** | Dev setup, test harness, lint, release procedure |
| Agent orientation | `docs/agent-kernel.md` | Unchanged in role; router table retargeted |

New `docs/guides/` (three files; content moved, not rewritten, except
transitions):

1. `docs/guides/python.md` — advanced API and embedding: delivery
   guarantees in practice, generators and thread-affinity, watchers,
   async integration, sidecar, reactor pointer, exact-ID insert /
   high-water / `find_message_ids`, command layer, embedding client
   pattern, activity waiters and backend-extension author notes that are
   Python-surface-shaped.
2. `docs/guides/configuration.md` — env-var catalog, project scoping
   (single precedence presentation, one worked example), `.broker.toml`,
   security notes, performance numbers and tuning.
3. `docs/guides/backends.md` — Postgres/Redis extensions, backend
   selection, benchmarking harness, "two backend shapes," API-version
   handshake.

Duplication resolution rule (apply everywhere, state once in
`docs/README.md`): *normative statement → spec-code link; orientation → one
short README restatement; runnable recipe → exactly one home (kernel for
agent recipes, `examples/` for shell scripts, guides for embedded
narrative); every other surface links the home.*

## Spec Baseline

- `197629e` — `docs/specs/product-section-registry.md`,
  `docs/specs/00-specs-index.md`, `docs/specs/10-cli.md` … `17-ops.md`,
  `docs/README.md`, `docs/program-theory.md` at plan authoring time.
- Plan type: **doc-migration only** — implementation against the registry's
  transition rule; it does not revise any spec.

## Proposed Spec Delta

None — this plan changes no intended behavior and no normative spec text.
Relocation only, per the registry transition rule ("Migration may relocate
promises but may not add, remove, narrow, broaden, correct, or deprecate
them"). Any wording change a reviewer or executor wants in a bound promise
is out of scope here and requires a separate owner-authorized contract
change.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Plan TB.2 / Appendix B (no spec) | New implementation doc named `07-storage-schema-and-claim-lifecycle.md` | Created as `09-storage-schema-and-claim-lifecycle.md` | Numbers 07 and 08 already exist in `docs/implementation/`; the plan was drafted without checking the index. Content and registration unchanged | n/a — plan-internal naming, no spec touched |
| Invariant 3 / TB.3 | Message-identity concern-row locus cell updated in the TB.3 commit | Cell still named README generation/insertion/cache sections until repaired post-slice-review (`f7e8f61` missed it; TOC row was updated) | Slice-review finding F3 (Phase B); repaired in the review-fix commit, one commit late | n/a — registry maintenance |
| Plan scope: relocation only (no spec) | Performance claims moved verbatim ("1000+ messages/second") | Replaced with owner-supplied measurements: ~1,700 ops/s mixed API use, ~30,000 ops/s optimized benchmark, ~20 ops/s CLI (Python startup), M2 Air / M4 Pro, plus not-the-bottleneck framing (README short form + configuration guide full form) | Owner instruction 2026-08-04 during execution; performance numbers are human catalog content, not a registered concern row, so no spec/registry gate applies | n/a — catalog content, owner-authorized |

(Empty at plan approval. Append one row per departure from this plan during
execution; must not remain `pending` past the completion gate.)

## Invariants (must remain true throughout)

1. **No promise mutation.** Relocation may move promises but may not add,
   remove, narrow, broaden, correct, or deprecate them
   (registry transition rule). Restated wording that a gate binds moves
   **verbatim**.
2. **Every gate stays green at every commit boundary.** A moved locus and
   its retargeted gate land in the same commit.
3. **Registry accuracy, same commit.** Every commit that moves a README
   locus also updates, in that same commit, (a) the affected concern-row
   "README anchor / locus" cells and (b) the affected TOC-ownership audit
   rows in `product-section-registry.md`. The registry never points at a
   heading that no longer exists, at any commit boundary. TD.1 is a final
   consistency audit only — it must find nothing to fix.
4. **No theory revision.** The README theory section is a non-governing
   distillation; `docs/program-theory.md` is not edited except to retarget
   README anchor links (editorial, not a `[DOM-16]` revision — flag to
   owner in review regardless).
5. **No normative spec edits.** The product specs `10-cli.md` … `17-ops.md`
   and the process spec `01-…` are not edited. Maintenance updates to
   `product-section-registry.md` (locus cells, TOC audit rows, Related
   Plans backlink) and `00-specs-index.md` (Related Surfaces) are in scope
   and follow those files' own rules.
6. **Kernel role preserved.** `docs/agent-kernel.md` stays a dense contract
   map; it gains no narrative content, only router-table retargets.
7. **Link integrity.** No dead relative link in any touched file at any
   commit boundary. `llms.txt` lists every new doc file (guides,
   CONTRIBUTING.md, the new implementation doc) in the same commit that
   creates it; `docs/README.md`'s roles table covers every new *tier*
   (the guides row) — it is a surface-role table, not a per-file list.
8. **Stable-code hygiene in new files.** New guides must not define
   headings containing `[DOM|SB|THEORY|ALT|REV]-…` codes, and every such
   code they reference must resolve to an existing definition. Enforcement
   note (corrected after review F4): the program-theory contract test's
   corpus scan collects heading *definitions* and `[ALT/REV]` records from
   `docs/**/*.md`, but validates *references* only from the theory file —
   it does not check guide references. This invariant is therefore
   enforced by the migration's own read-only audit (Testing plan) and by
   review, not by an existing test.
9. **PyPI rendering.** `README.md` is the PyPI long description
   (`pyproject.toml` `readme = "README.md"`). The restructured README is
   the next release's PyPI landing page. Links intended to work on PyPI
   must be absolute GitHub URLs (see Decision D2).

## Hidden couplings (census — verify in Task 0, do not trust from memory)

Mechanical binds discovered 2026-08-04:

- `tests/test_documented_exit_codes.py` — splits README on `### Exit Codes`
  (stays) and `### Command layer` (**moves** to `docs/guides/python.md`;
  gate must retarget).
- `tests/test_agent_kernel_contract.py` — binds the two kernel split
  headings plus literal kernel phrases, llms.txt structure, and
  public-symbol assertions (Appendix A row). All bind content this
  migration leaves unchanged: kernel edits are router-table retargets
  only, llms.txt edits are additions only.
- `tests/test_delivery_contract_sb_delivery.py`,
  `test_timestamp_selection_contract_sb_select.py`,
  `test_persistence_io_contract_sb_io.py`,
  `test_operations_contract_sb_ops.py`,
  `test_python_library_api_contract_sb_api.py`,
  `test_broadcast_contract_sb_bcast.py`,
  `test_message_identity_contract_sb_id.py` — assert spec-path links and/or
  normalized restatement phrases present in `README`, `KERNEL`, and `LLMS`.
  The README must **retain a link to every product spec** and any bound
  restatement sentence (inventory the exact assertions per file in Task 0).
  `test_message_identity_contract_sb_id.py` additionally compares
  normalized README text against spec phrases — the "Timestamps as Message
  IDs" trim must preserve the bound sentences verbatim.
- `tests/test_broadcast_contract_sb_bcast.py:156` — asserts `[BCAST-` never
  appears in README (only `[SB-BCAST-`); keep during rewrite.
- `tests/test_program_theory_contract.py` — collects `[ALT/REV]` record
  definitions and stable-code heading definitions corpus-wide (`README.md`
  + `docs/**/*.md` + `skills/**/*.md`), but validates stable-code
  *references* only from the theory file (review F4). New guide files
  join the definition corpus, so they must not define bracketed-code
  headings; their references are checked by this plan's read-only audit,
  not by the test (invariant 8).
- `docs/program-theory.md` links three README anchors:
  `#embedding-simplebroker-in-your-project` (THEORY-2) and
  `#advanced-custom-extensions` (THEORY-3 backend row) retarget to guide
  anchors in TB.3; `#real-time-queue-watching` (THEORY-3 watcher row) is
  **not** retargeted — its README heading survives with text preserved
  (TB.3 decision). Retargets land in the same commit as the moves
  (invariant 4).
- `docs/specs/product-section-registry.md` — README-locus column rows for
  Command Options, Queue Aliases, Critical Safety, Timestamps as Message
  IDs, Python API / Embedding / Command layer / Sidecar, Fan-out, Env/
  Project-scoping catalogs; plus the whole TOC-ownership audit table.
- `docs/agent-kernel.md` "Where to go next" — routes "Full
  command/flag/env contract" to `README.md`; env/config depth moves to
  `docs/guides/configuration.md`.
- `llms.txt` — must list the three guides and `CONTRIBUTING.md`.
- `docs/README.md` — roles table gains a `docs/guides/` row and the
  duplication resolution rule.
- `pyproject.toml` — README is the PyPI long description (invariant 9).
- `docs/coalescing.md` / `bin/coalesce-check` — not touched; plan adds one
  index row only.

## Decisions required from owner before execution

- **D1 — guide tier location and names.** Proposed: `docs/guides/{python,
  configuration,backends}.md`. Alternative: flat `docs/` files. Recommend
  the directory: it makes the how-to quadrant legible as a tier.
- **D2 — README link policy for PyPI.** Proposed: absolute GitHub URLs in
  the README router table and spec links (relative links do not resolve on
  PyPI; the status quo is already broken there). In-repo docs keep
  relative links. Alternative: accept broken PyPI links as today.
- **D3 — theory-section stance.** Confirm the README theory section is a
  non-governing distillation under the existing "orientation" role in
  `docs/README.md`, so divergence is a doc bug, never a shadow theory
  revision.
- **D4 — release/contributing home.** Proposed: single `CONTRIBUTING.md`
  at root containing dev setup + release procedure. Alternative: split
  `RELEASING.md`. Recommend single file; the release section is
  maintainer-only but small enough not to warrant a second file.

## Dependency-ordered tasks

Every task ends with its stop-gate check and a green targeted test run.
Meaningful slices (end of Phase B, end of Phase D) get an independent
review pass per [DOM-11] before proceeding.

### Phase 0 — bind inventory (read-only; blocking)

- T0.1 Enumerate the README/kernel/llms assertions in the nine contract
  test files plus `test_program_theory_contract.py` that are **reachable
  from the surfaces this migration changes** (README sections being
  moved/rewritten; llms.txt additions; kernel router table) — the same
  scope Appendix A records; assertions binding content the plan leaves
  untouched are noted once as out-of-scope, not enumerated. Record
  file → assertion → disposition (stays / retargets / verbatim-move) in
  Appendix A of this plan.
- T0.2 Extract the exact bound sentences (message-identity restatements,
  spec-path links) into Appendix A so later phases can verify verbatim
  moves mechanically (`grep -F`).
- **Stop gate:** if any assertion cannot be classified, stop and revise
  this plan before touching prose.
- Comprehension check (answer in the appendix): *Which README headings are
  split targets of `test_documented_exit_codes.py`, and what happens to a
  split when the heading is absent?* (Answer expected: `### Exit Codes`,
  `### Command layer`; `str.split(...)[1]` raises `IndexError` → test
  errors, so a heading rename without gate retarget fails loudly, not
  silently.)

### Phase A — new homes, no README changes yet

- TA.1 Create `CONTRIBUTING.md` — **copy-only**: Development &
  Contributing + Releases content copied verbatim; README untouched in
  this phase (the README-side cutover is TB.5).
- TA.2 Create `docs/guides/python.md`, `configuration.md`, `backends.md`
  with moved content per the rehoming map (Appendix B). Transitions and
  intros may be new prose; promises move verbatim.
- TA.3 Register the new files: `docs/README.md` roles table + resolution
  rule, `llms.txt` entries, `docs/specs/00-specs-index.md` Related
  Surfaces.
- **Stop gate:** guides must not yet be linked from README; both copies of
  moved text exist simultaneously in this phase (harmless: README still
  wins nothing — registry loci still point at README). Run full contract
  test selection; expect green with zero retargets.

### Phase B — cut over the moved loci (one commit per concern family)

Order: lowest-coupling first. Registry vocabulary (fixed after review F5):
"concern rows" are the eight rows of the main registry table (their
"README anchor / locus" cells); "TOC rows" are the rows of the
TOC-ownership audit table. Each TB task names exactly which of each it
updates, in the same commit as the move (invariant 3).

- TB.1 Configuration/scoping/env → replace README sections with the
  common-settings table + link (target-architecture row); retarget kernel
  router row ("Full command/flag/env contract"). Registry: no concern-row
  cell changes (env/scoping are human-catalog residual, not concern rows);
  TOC rows changed: "Environment Variables / Project Scoping catalogs".
- TB.2 Backends/benchmarking + Architecture `<details>` blocks per
  Appendix B → guides/implementation destinations (creates
  `docs/implementation/07-storage-schema-and-claim-lifecycle.md`,
  registered in the implementation index **and** `llms.txt` in the same
  commit — invariant 7 lists every new file).
  Registry: concern-row cells unchanged; TOC rows: none (these README
  sections are human-entry, per the registry's own audit preamble).
- TB.3 Advanced Python/embedding/command layer (including
  `### Advanced: Custom Extensions`) → strip from README, leave
  API-tour + links; retarget `test_documented_exit_codes.py`
  `### Command layer` split to `docs/guides/python.md`; retarget
  program-theory `#embedding-simplebroker-in-your-project` and
  `#advanced-custom-extensions` links. **Watcher locus (decided at plan
  time, review F5):** README keeps a compact "Real-time Queue Watching"
  subsection — heading text preserved so the theory link
  `#real-time-queue-watching` keeps resolving and is **not** retargeted
  (F5 round 2) — with three modes, the pipe-behavior paragraph, and
  `[SB-DELIVERY-*]` links; polling-strategy and move-mode depth move to
  `guides/python.md`. Registry (decided, no conditionals): concern-row
  cells updated — Delivery (locus adds guide), Python library/embedding
  (locus becomes README API tour + `guides/python.md`); TOC rows changed:
  "Real-time Queue Watching / Pipe / Move Mode", "Python API / Delivery
  guarantees", "Sidecar / Command layer / Embedding", and "Exact IDs /
  high-water / generate timestamp" (deep content → `guides/python.md`;
  bound identity sentences stay in README Core Concepts; the TOC row
  names both loci). "Queue metadata" TOC row: **no change** — the
  metadata examples stay in the README API tour per Appendix B.
- TB.4 Shell patterns → long scripts become `examples/` files.
  **Deletion-as-duplication rule (review F2):** a README script may be
  deleted rather than moved only when the plan's move ledger cites the
  exact surviving text (file + section) that preserves each of its unique
  statements, and the slice review confirms semantic equivalence. The
  two worker recipes (README `safe-worker.sh` peek-and-acknowledge;
  README resilient-worker checkpointing) are **not** assumed duplicates
  of `examples/resilient_worker.sh` / kernel move-to-inflight: their
  unique statements (checkpoint atomicity via temp-file rename;
  per-message acknowledge-by-delete) must be accounted for row by row.
  Registry: no concern-row or TOC changes.
- TB.5 Contributor cutover (review F6): README Development & Contributing
  + Releases sections → stub linking `CONTRIBUTING.md`. Registry: none
  (human entry).
- **Stop gates (each TB task):** targeted contract tests green in the same
  commit; `grep -F` proves bound sentences moved verbatim; move-ledger
  rows written for every removed block; read-only link/anchor audit clean
  (a recorded `grep`/`ls` audit command, not new tooling — review F9).
- **Independent review** after Phase B: different agent family, checklist =
  invariants 1–3, 7, 8 against the diff.

### Phase C — README top rewrite (theory + router)

- TC.1 Write the theory section (~80 lines): distill `[THEORY-1/2/5]` +
  the four Design Philosophy rules; fold Features/Use Cases into
  Recommended For; keep the ownership-boundary paragraph (it is bound by
  program-theory evidence rows — verify via Phase 0 inventory).
  Constraint: must not reintroduce wording the theory contract forbids
  (e.g., the overbroad "Not for: Distributed systems" phrasing retired by
  `[REV-THEORY-002]`).
- TC.2 Build the "Going further" router table (one row per topic → guide /
  spec family / kernel / examples / CONTRIBUTING).
- TC.3 Trim catalog-adjacent deep prose per Appendix B (timestamp string
  forms → one line + `[SB-CLI-5]`; cross-thread poisoning → three
  sentences + `[SB-DELIVERY-6]` + implementation doc 04; broadcast prose →
  selector table + `[SB-BCAST-*]`; duplicate worker scripts → kernel/
  examples pointers).
- **Stop gate:** `tests/test_program_theory_contract.py` and the full
  contract selection green; README length ≤ ~750 lines; every `[SB-*]`
  spec file still linked from README; TOC rows affected by TC.3 trims
  updated in the same commit (invariant 3).

### Phase D — closure

- TD.1 Registry consistency **audit only** (review F1): verify every
  concern-row locus cell and TOC row already matches the final README —
  per invariant 3 this audit must find nothing to fix; a discrepancy is a
  Deviation Log entry, not a silent repair. Add this plan to the
  registry's `## Related Plans` list (backlink rule, writing-plans
  runbook).
- TD.2 `CHANGELOG.md` Documented entry (doc restructure is user-visible).
- TD.3 Full `uv run pytest` + `python3 bin/check-dom15-fixtures` (the
  DOM-15 fixture check is the standing house floor for docs changes —
  AGENTS.md harness note — retained per commit for that reason, not as
  migration-specific signal; review F9).
- TD.4 Independent completion review ([DOM-11]); incorporate or answer.
- TD.5 Flip this plan's index row to `completed` in the same change as the
  completion claim; record verification evidence below.

## Testing plan

- **What the gates actually prove (corrected after review F3):** the
  contract tests preserve the *known mechanical binds* — a handful of
  literal headings, spec-path links, and bound sentences (Appendix A).
  They are necessary but nowhere near sufficient: most promises in the
  moved sections are not test-bound. Run the targeted selection at every
  commit; full suite at Phase D.
- **The proof for unbound promises is the move ledger + semantic review.**
  During execution, every removed README block gets a ledger row
  (appended to Appendix C of this plan): source section → destination
  file/anchor (or "deleted-as-duplicate" with the exact surviving text
  cited) → verified-by (commit, reviewer). The Phase B and Phase D
  independent reviews check the ledger against the diffs for semantic
  equivalence. Do **not** add shallow substring gates to simulate
  coverage (review F3).
- **Guide reference audit (review F4):** before each Phase B/C commit,
  run a read-only audit that every bracketed stable code referenced in
  `docs/guides/` and `CONTRIBUTING.md` resolves to an existing heading
  definition, and that no guide heading defines one. Recorded audit
  command, not new tooling.
- **Do-not-weaken rule (anti-mocking analog):** gates may be *retargeted*
  (new file/heading), never deleted, never loosened to substring-anywhere,
  never pointed at a stub that restates less than the bound sentence. A
  retarget commit must show the assertion still binds the same sentence at
  its new locus.
- Verbatim-move verification is mechanical: `grep -F "<bound sentence>"`
  against the destination file, recorded in the plan appendix per TB task.
- Link integrity: audit all relative links in touched files resolve
  (script or manual `ls` sweep recorded as evidence).
- `python3 bin/check-dom15-fixtures` on every docs-only commit (house
  rule).

## Verification and gates (evidence to record at completion)

- Final line counts (README target ≤ ~750; guides; CONTRIBUTING).
- Targeted + full pytest output.
- Registry diff showing locus columns and TOC table consistent with final
  README.
- Link-audit output for README, guides, kernel, llms.txt, program-theory.
- Independent review notes (Phase B and Phase D).

## Rollback and rollout sequencing

- Docs-only; every phase is an independently revertible commit series
  (`git revert`), no data or storage format touched, no one-way doors.
- Sequencing matters only for invariant 2: a locus move and its gate/
  registry/theory-link retargets are one commit. Phases A→D are ordered so
  that reverting any suffix of phases leaves a consistent (if
  partially-migrated) tree: guides may exist unlinked (post-A revert of B)
  without breaking anything.
- PyPI exposure happens only at the next release; if the restructured
  README should not ship with an imminent release, hold Phase C until
  after that release (release sequencing decision belongs to the owner).

## Out of scope

- Any spec wording or spec structure change; any new/changed promise.
- Kernel restructure beyond router-table retargets.
- `examples/` script rewrites beyond relocation/dedup.
- Program-theory content changes ([DOM-16] path if ever needed).
- README marketing rewrite beyond the three-purpose structure (tone
  polish is in; repositioning is not).
- Translating docs, doc-site tooling, mkdocs/sphinx adoption.
- Plan coalescing, lessons harvesting, unrelated doc-debt.

## Independent review loop

- Reviewer selection follows `docs/implementation/03-agent-inventory.md`
  and `skills/call-agent/SKILL.md`: **Codex** (live, OS-enforced read-only
  sandbox) is the different-family gate reviewer for the plan and for the
  completion review; a same-family Claude reviewer in a separate review
  role may supplement. Grok is not currently suitable as a sole gate
  (inventory note, 2026-07-29).
- Reviewer inputs per [DOM-11] / review-loops §3: this plan (embedded
  verbatim in the brief), the registry, `docs/README.md`,
  `docs/program-theory.md`, the hardening runbook, the gate test files
  named in Appendix A, and the README.
- Prompt stance: review-loops §4 (errors, bad ideas, latent ambiguities,
  performative overengineering; PASS/BLOCKED from the two gate questions).
- Feedback returns to the plan author, who dispositions every finding in
  the Review Log below (accepted-and-addressed / rejected-with-reasoning /
  out-of-scope-with-reasoning) before execution proceeds.
- Plan review (pre-execution): focus on invariants completeness, gate
  census correctness (Appendix A), rehoming-map completeness (Appendix B),
  and D1–D4 recommendations.
- Slice reviews: end of Phase B and Phase D as above.
- Reviewer instructions: diff-driven; verify verbatim moves by `grep -F`
  spot checks; attempt to find a promise that changed meaning in transit —
  a single confirmed instance blocks. Also check new prose against the
  Simplified Technical English standard above (sentence length, active
  voice, one term per concept) and flag violations as review findings.

## Review Log and Dispositions

Findings from independent reviews land here verbatim (ID, severity,
finding); each receives a disposition row before execution proceeds:

| ID | Reviewer | Severity | Finding (short) | Disposition |
|----|----------|----------|-----------------|-------------|
| F1 | Codex (2026-08-04, round 1, verdict BLOCKED) | P1 | Execution order violated the same-commit registry invariant: TOC table rewrite deferred to TD.1 while Phase B moved loci | **Accepted, fixed.** Invariant 3 now requires concern-row cells and TOC rows in the same commit as each move; TB tasks enumerate exactly which; TD.1 is audit-only and must find nothing |
| F2 | Codex | P1 | Appendix B incomplete: no exact destination for Concurrency/Delivery, Security Considerations, Custom Extensions; "placement check" not a destination; TB.4 dedup risked promise loss | **Accepted, fixed.** Appendix B now has one row per block with exact destinations (incl. new `docs/implementation/07-storage-schema-and-claim-lifecycle.md`); TB.4 deletion-as-duplication requires ledger citation of exact surviving text + semantic-equivalence review |
| F3 | Codex | P2 | Testing plan overclaimed contract tests as "mechanical proof no promise moved incorrectly" | **Accepted, fixed.** Reworded to "preservation of known mechanical binds"; the proof for unbound promises is the Appendix C move ledger + semantic review; no shallow substring gates added |
| F4 | Codex | P2 | Invariant 8 claimed the theory contract test validates guide references; it validates references from the theory file only | **Accepted, fixed.** Invariant 8, Appendix A row, and comprehension Q3 corrected; enforcement is the plan's read-only reference audit + review |
| F5 | Codex | P2 | Final env and watcher loci contradictory/deferred; concern rows conflated with TOC rows | **Accepted, fixed.** Decided at plan time: README keeps ~6-key common-settings table (full env catalog → configuration guide); README keeps compact watching subsection (depth → python guide). Phase B defines the concern-row/TOC-row vocabulary and enumerates exact cells per task |
| F6 | Codex | P2 | Contributor cutover had no executable step (TA.1 self-contradictory) | **Accepted, fixed.** TA.1 is copy-only; new TB.5 owns the README-side stub cutover |
| F7 | Codex | P2 | Invariant 5 "specs out of scope entirely" contradicted required registry/spec-index edits | **Accepted, fixed.** Narrowed to "no normative product/process spec edits"; registry and `00-specs-index.md` maintenance explicitly in scope |
| F8 | Codex | P3 | Appendix A kernel-test row understated that test's other binds (llms structure, kernel phrases, symbols) | **Accepted, fixed.** Row corrected; census scope explicitly limited to assertions reachable from surfaces this migration changes, with rationale (kernel content unchanged; llms.txt additions only) |
| F9 | Codex | P3 | Per-commit `check-dom15-fixtures` adds no migration signal; link-checker tooling risks permanent machinery | **Partially accepted.** Link audit demoted to a recorded read-only command, no new tooling (accepted). Per-docs-change DOM-15 fixture run retained: it is the standing house floor in AGENTS.md, not plan-added ceremony (rejected with reasoning); TD.3 notes this |
| — | Codex | obs | README's strong pre-existing claims (exactly-once, security, performance) may deserve accuracy review | **Out of scope with reasoning.** Predates this plan; migration must move, not audit, those claims. Noted for the owner as a possible follow-up |
| SB1–SB5 | Codex (Phase B slice review, verdict blocker: F1–F5) | P2×2, P3×3 | F1 checkpoint-worker dedup not equivalent (non-atomic trap write; continues after delete failure); F2 bounded BATCH_SIZE promise lost; F3 message-identity concern-row locus stale (invariant-3 miss at TB.3); F4 ledger omitted trusted-config paragraph's surviving home; F5 guide watcher snippet not self-contained | **All accepted, fixed** in the review-fix commit: resilient_worker.sh gains atomic trap checkpoint, stop-on-delete-failure, and BATCH_SIZE bound; registry locus cell repaired (+ deviation-log row); ledger row cites configuration guide; watcher snippet carries its handler definitions. Round-2 slice verification follows |
| R4 | Codex (round 4, verdict **PASS**) | — | All fixes verified; no new contradiction found | **Gate review closed.** Plan review per review-loops §4: round 1 BLOCKED (F1–F9), rounds 2–4 scoped verification of accepted fixes, final PASS 2026-08-04 |
| R3 | Codex (round 3, verdict FAIL on one item) | — | Hidden Couplings anchor bullet and comprehension Q2 still said all three theory links retarget | **Accepted, fixed.** Both now state the watching link is not retargeted and why |
| R2 | Codex (round 2, verdict FAIL) | — | Residual contradictions after round-1 fixes: TB.3 conditionals + wrong watching-link retarget (F1/F5), Hidden Couplings still carried the old corpus-scan and kernel-binds claims (F4/F8), T0.1 scope contradicted Appendix A (F8), new implementation doc vs invariant 7 file-listing (F2) | **Accepted, fixed.** TB.3 decided with no conditionals and the watching link explicitly not retargeted (README heading preserved); Hidden Couplings rows rewritten to match Appendix A; T0.1 narrowed to reachable assertions; TB.2 registers the implementation doc in llms.txt; invariant 7 reworded (llms.txt per-file; docs/README per-tier) |

## Fresh-eyes review

(To be completed by a reviewer who did not author this plan: read the
final README top-to-bottom as a first-time adopter; read one guide
end-to-end; note any point where you had to open a second document to
finish a task the first document promised to cover. Record findings here.)

## Comprehension questions (for the executing agent)

1. When a README heading bound by a `str.split` gate is renamed without a
   gate retarget, how does the failure surface? (Loud `IndexError`, not a
   silent pass — which is why retargets must be same-commit, not
   follow-up.)
2. Which three program-theory links point into the README, which two are
   retargeted (and in which task), and why is the third left alone?
   (Answer: embedding and custom-extensions retarget in TB.3;
   `#real-time-queue-watching` stays because the README keeps that
   heading verbatim.)
3. Why must new guide files avoid headings containing bracketed stable
   codes, and what enforces it? (Answer: a guide heading would add a new
   definition to the corpus that nothing owns; enforcement is this plan's
   read-only reference audit and review — the theory contract test
   collects definitions corpus-wide but validates references only from
   the theory file, so it does NOT catch guide mistakes.)
4. Why do promises move verbatim even when the wording could be improved?
   (Registry transition rule: relocation may not correct promises; wording
   improvements are a separate owner-authorized contract change.)

## Appendix A — gate/bind census (pre-verified during plan drafting, 2026-08-04)

This census was verified against the gate files while drafting the plan.
Phase 0 remains a required step for the executor: re-run the verification
against the tree at execution time before any prose moves (binds can drift
between plan approval and execution).

README binds (all other `README` mentions in the gate files are the path
constant only):

| Bind | Source | Disposition |
|------|--------|-------------|
| `### Exit Codes` heading exists; section (to next `\n## `) enumerates exactly `- \`0\``/`- \`1\``/`- \`2\`` bullets and contains `docs/specs/10-cli.md` | `tests/test_documented_exit_codes.py:12–34` | stays in README; keep bullet forms and link in section |
| `### Command layer` heading exists (split raises `IndexError` if absent); its section must not contain `` `124` `` | `tests/test_documented_exit_codes.py:37–41` | TB.3 moves the section → retarget the test to `docs/guides/python.md` in the same commit |
| README contains literally: `docs/specs/13-message-identity.md`, `[SB-ID-1]`, `[SB-ID-5]`, `14-timestamp-selection.md` | `tests/test_message_identity_contract_sb_id.py:222–224,242` | retain in condensed Core Concepts |
| Normalized README contains: `Broker-generated message IDs are positive`; `Exact selectors still accept zero`; `move\` preserves IDs` or `move preserves IDs`; `19 decimal digits` | same file `:234–240` | frozen sentences — keep verbatim in README Core Concepts |
| README must NOT contain `High 52 bits: microseconds` | same file `:241` | preserve prohibition |
| README contains `docs/specs/11-delivery.md` | `tests/test_delivery_contract_sb_delivery.py:91–92` | retain (safety section link) |
| README contains `docs/specs/14-timestamp-selection.md` and `SB-SELECT` | `tests/test_timestamp_selection_contract_sb_select.py:50–53` | retain (filter subsection) |
| README contains `docs/specs/15-persistence-io.md` | `tests/test_persistence_io_contract_sb_io.py:49–51` | retain (dump/load rows) |
| README contains `docs/specs/17-ops.md` | `tests/test_operations_contract_sb_ops.py:49–51` | retain (command catalog) |
| README contains `docs/specs/16-python-library-api.md` | `tests/test_python_library_api_contract_sb_api.py:58–60` | retain (API tour) |
| README contains `docs/specs/12-broadcast.md`; must NOT contain `[BCAST-` | `tests/test_broadcast_contract_sb_bcast.py:155–156` | retain link; preserve prohibition |
| kernel binds: the two split headings above, plus (census scope note, review F8) literal kernel phrases, llms.txt structure, and public-symbol assertions elsewhere in the file | `tests/test_agent_kernel_contract.py` | kernel content and llms.txt existing lines are unchanged by this migration (kernel: router-table retargets only; llms.txt: additions only), so these binds are unaffected; the census deliberately covers only assertions reachable from surfaces this plan changes |
| llms.txt contains all spec paths + `[SB-ID-1]`/`[SB-ID-5]`, `[SB-BCAST-1]`/`[SB-BCAST-6]`, `SB-SELECT` | same gate files, `LLMS` path | additions only; never remove existing lines |
| corpus scans: unique `[ALT/REV]` record definitions collected from README + `docs/**/*.md` + `skills/**/*.md`; stable-code *references* validated **from the theory file only** (corrected per review F4 — guide references are NOT test-checked) | `tests/test_program_theory_contract.py:243,405` | guides reference only existing codes and define none in headings; enforced by this plan's read-only audit + review, not by the test |
| program-theory README anchors: `#embedding-simplebroker-in-your-project` (THEORY-2), `#real-time-queue-watching`, `#advanced-custom-extensions` (THEORY-3) | `docs/program-theory.md:81,100,103` | embedding + custom-extensions links retarget in TB.3; the watching link is NOT retargeted — its README heading survives (TB.3 decision). Markdown links are not test-bound; invariant 7 applies |
| registry locus column + TOC table | `docs/specs/product-section-registry.md` | updated in the same commit as each locus move (invariant 3); TD.1 audits only and must find nothing |
| PyPI long description | `pyproject.toml:9` | D2: absolute GitHub URLs in README router/spec links |

Phase 0 comprehension answer: `test_documented_exit_codes.py` splits on
`### Exit Codes` and `### Command layer`; `str.split(...)[1]` on a missing
heading raises `IndexError`, so a heading move without a same-commit gate
retarget fails loudly, never silently.

## Appendix B — rehoming map (README section → destination)

| Current README section | Destination | Notes |
|---|---|---|
| Intro / badges / pitch | README (rewrite) | |
| Recommended For; Features; Use Cases; Good for/Not for | README §Theory (merged) | SQLite-whentouse model; keep ownership-boundary paragraph |
| Installation; Quick Start | README | unchanged in substance |
| Command Reference (Global Options, Commands, Queue Aliases, Command Options, Exit Codes) | README catalogs | trim deep prose to spec links; `### Exit Codes` heading preserved |
| Critical Safety Notes | README (condensed) + kernel recipe link | inline scripts out; `safe-worker.sh` → pointer |
| Core Concepts (IDs, JSON, filtering) | README (condensed) | preserve bound identity sentences verbatim |
| Common Patterns | README keeps 2–3 short; long scripts → `examples/` | deletion-as-duplication only with ledger citation of exact surviving text (TB.4 rule); the checkpointing worker's unique statements (temp-file-rename checkpoint atomicity, per-message ack-by-delete) must survive somewhere named |
| Real-time Queue Watching / Pipe behavior / Move Mode | split (decided): README keeps compact subsection — three modes, pipe-behavior paragraph, `[SB-DELIVERY-*]` links; polling-strategy + move-mode depth → `guides/python.md` | TOC row updated same commit (TB.3) |
| Python API basics (Queue, watcher, context manager) | README §API tour | ~100 lines |
| Python API advanced (delivery guarantees detail, generators, poisoning, metadata, latest-pending, generate-timestamp, exact-ID insert, high-water, find_message_ids, sidecar, reactor, async, open_broker, waiters) | `guides/python.md` | poisoning compresses in README to 3 sentences + links |
| Embedding + Command layer | `guides/python.md` | program-theory + registry + exit-codes-test retargets |
| Performance & Tuning; Cross-Backend Benchmarking | `guides/backends.md` (benchmark) + `guides/configuration.md` (tuning) | |
| Environment Variables; Project Scoping (all subsections) | `guides/configuration.md`; README keeps the ~6-key common-settings table + link (decided, review F5) | one precedence presentation; one worked example |
| Architecture: Design Philosophy `<details>` | README §Theory | the four rules feed the theory section |
| Architecture: Database Schema and Internals `<details>` | new `docs/implementation/07-storage-schema-and-claim-lifecycle.md`, registered in `docs/implementation/00-implementation-index.md` (created in TB.2) | verify against existing impl docs first; if a block is already covered there, ledger-cite the surviving text instead of duplicating |
| Architecture: Concurrency and Delivery Guarantees `<details>` | README §Theory keeps a 2–3 sentence restatement + `[SB-DELIVERY-*]` link; the message-lifecycle enumeration → `docs/implementation/07-…` | no promise deleted; ledger rows per sentence group |
| Architecture: Security Considerations `<details>` | `guides/configuration.md` §Security | joins the Project Scoping security notes |
| Architecture: Things That Look Weird but Aren't `<details>` | config-flavored Q&A (why 32 settings, sync-mode default) → `guides/configuration.md`; internals Q&A (phaselock, claimed-before-vacuum, Redis parallel core) → `docs/implementation/07-…` | |
| Advanced: First-Party Backend Extensions | `guides/backends.md` | |
| Advanced: Custom Extensions (BrokerCore/BrokerDB boundary note + priority-queue example) | `guides/python.md` §Extensions | program-theory `#advanced-custom-extensions` link retargets here in TB.3 |
| Development & Contributing; Releases | `CONTRIBUTING.md` | D4 |
| License; Acknowledgments | README | |

## Appendix C — move ledger (execution artifact; empty at approval)

One row per README block removed during Phase B/C. "Deleted-as-duplicate"
rows must cite the exact surviving text (file + section). The Phase B and
Phase D reviews verify this ledger against the diffs for semantic
equivalence (Testing plan).

| Source (README section, pre-move lines) | Destination (file + anchor) or deleted-as-duplicate citation | Commit | Verified by |
|-----------------------------------------|--------------------------------------------------------------|--------|-------------|
| `### Environment Variables` full catalog (1577–1660) | `docs/guides/configuration.md` §Environment variables (verbatim); README keeps 6-key common-settings table + link | TB.1 | Phase B slice review |
| `## Project Scoping` all subsections (1662–2057): basic/global scoping, DB/config names, error behavior, init, precedence list + notes + examples, security notes, common use cases | `docs/guides/configuration.md` §Project scoping + §Security (verbatim except: decision flowchart deleted-as-duplicate — its content is the §Precedence rules numbered list + notes, same guide; five Common Use Cases collapsed to §Worked example with an explicit sentence naming the covered shapes) | TB.1 | Phase B slice review |
| `### Cross-Backend Benchmarking` (orig 1546–1575) | `docs/guides/backends.md` §Cross-backend benchmarking (verbatim); README keeps 3-line pointer | TB.2 | Phase B slice review |
| Architecture `<details>`: Database Schema and Internals (orig 2086–2106) | `docs/implementation/09-storage-schema-and-claim-lifecycle.md` §Database schema (verbatim) | TB.2 | Phase B slice review |
| Architecture `<details>`: Concurrency and Delivery Guarantees (orig 2108–2125) | `docs/implementation/09-…` §Concurrency and delivery realization (verbatim; README theory restatement lands in Phase C) | TB.2 | Phase B slice review |
| Architecture `<details>`: Security Considerations (orig 2127–2139) | deleted-as-duplicate: `docs/guides/configuration.md` §General security considerations carries the identical bullet list (copied in Phase A) | TB.2 | Phase B slice review |
| Architecture `<details>`: First-Party Backend Extensions (orig 2141–2333) | `docs/guides/backends.md` (packages, .broker.toml, backend shapes, runner/transaction ownership, fork note — verbatim) + `docs/guides/python.md` §Activity waiters (waiter/hook material — verbatim) + `docs/guides/configuration.md` §General security considerations (trusted-config paragraph, orig 2326–2332 — verbatim, copied in Phase A; ledger citation added per slice-review F4) | TB.2 | Phase B slice review |
| Architecture `<details>`: Things That Look Weird but Aren't (orig 2335–2364) | split verbatim: settings-count + sync-mode Q&A → `docs/guides/configuration.md` §Environment variables; phaselock + claimed-rows Q&A → `docs/implementation/09-…`; Redis-parallel-core Q&A → `docs/guides/backends.md` §Backend authoring | TB.2 | Phase B slice review |
| Performance & Tuning intro bullets | retained in README (orientation restatement); fuller home `docs/guides/configuration.md` §Performance and tuning | TB.2 | Phase B slice review |
| `## Real-time Queue Watching` depth (orig 717–775): polling-strategy bullets, Move Mode subsection | `docs/guides/python.md` §Watchers in depth + §Move mode (verbatim); README keeps heading, three modes, example, pipe-behavior subsection, move one-liner | TB.3 | Phase B slice review |
| Python API advanced (orig 776–1411): find_message_ids/delete_many, delivery-guarantee depth incl. cross-thread poisoning, peek include_claimed, python dump/load, latest_pending_timestamp, generate_timestamp, insert_messages, high-water tracking, watcher error contract, run_in_thread, context manager, async wrapper, open_broker block, Custom Extensions, Sidecar, Reactor | `docs/guides/python.md` — one section per topic, verbatim; README keeps basic Queue example, delivery summary, Queue metadata (verbatim), compact watcher example, and a Going-deeper list | TB.3 | Phase B slice review |
| `## Development & Contributing` + `### Releases` (orig 2366–2513) | `CONTRIBUTING.md` (verbatim, copied in Phase A); README keeps 6-line stub with the contributing principles sentence | TB.5 | Phase B slice review |
| Critical Safety `safe-worker.sh` inline script (orig 397–419) | new `examples/safe_worker.sh` (verbatim; comments extended with kernel pointer); README keeps 4-line pointer | TB.4 | Phase B slice review |
| Unix Tool Integration single-message move-reservation script (orig 600–615) | deleted-as-duplicate: `docs/agent-kernel.md` "Minimal use recipes" move-to-inflight loop preserves the pattern (atomic move, process, delete-by-id, DLQ branch) with concurrency guidance; README keeps kernel pointer + the distinct `move --all --json` ndjson loop | TB.4 | Phase B slice review |
| Common Patterns `Resilient Worker with Checkpointing` block (orig 642–715) | deleted-as-duplicate: `examples/resilient_worker.sh` preserves the unique statements — atomic temp-file+rename checkpoint (`save_checkpoint`, incl. the signal trap), per-message acknowledge-by-delete with stop-on-delete-failure, and the `BATCH_SIZE` bound (restored per slice-review F2); README keeps 3-line pointer. Cite by name, not line number — the script has been edited since the move | TB.4 | Phase B slice review + round-2 |
| `## Embedding SimpleBroker in Your Project` (orig 1413–1498) + `### Command layer` (orig 1500–1537) | `docs/guides/python.md` §Embedding (heading text preserved for theory anchor) + §Command layer (heading preserved for retargeted gate); README keeps 2-paragraph embedding summary. Gate `test_documented_exit_codes.py::test_command_layer_…` retargeted to the guide; theory embedding + custom-extensions links retargeted (watching link untouched) | TB.3 | Phase B slice review |

## Related Plans

- `docs/plans/2026-07-30-product-documentation-cutover-plan.md` (completed
  — created the canonical-spec layer this plan builds on)
- `docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md`
  (active — owns the theory surface this plan distills but does not edit)
- retired: 2026-07-27-information-architecture-improvement-plan — source
  `36e2f356`; historical roadmap superseded before execution
