# SimpleBroker Docs

Design, planning, and specification record for SimpleBroker.

## Product documentation ownership

Product behavior is governed by the **layered source-of-truth system**
registered in `docs/specs/product-section-registry.md`.

**As of the product-documentation cutover (Phases 1–6), every current product
concern family is `canonical-spec`.** Exact intended behavior lives under
`docs/specs/10-cli.md` … `17-ops.md` with stable `[SB-*]` codes. The root
`README.md` is the human product entry: orientation, catalogs, examples, and
short restatements that **link** the winning spec. It is not a competing SoT
for registered families.

| Surface | Role |
|---------|------|
| `docs/program-theory.md` | Current conceptual account: purpose, mental model, concept ownership, durable principles/non-goals, tensions, and revisions; **not** exact behavior authority |
| Root `README.md` | Human entry, command/env/API **catalogs**, examples, and concise restatements with links to canonical specs |
| `docs/specs/` product sections (`[SB-*]`) | **Normative** for every registered product concern (`canonical-spec` in the registry) |
| `CHANGELOG.md` | Behavior deltas for published releases |
| `docs/agent-kernel.md` | Agent-oriented **view** of use-level rules; must not invent obligations beyond the winning SoT |
| `llms.txt` | Machine-readable link index (not normative) |

**Conflict rule:** For a registered product concern, the `canonical-spec`
section in `docs/specs/product-section-registry.md` wins over README prose.
README may restate short tables and must link the section code. The state
vocabulary `readme-only` | `draft-spec` | `canonical-spec` remains for
**future** concerns; a new family may start as `readme-only` until promoted.

Program theory precedes and informs those contracts but does not override
them. It owns conceptual identity and design judgment. Implementation docs
explain concrete realization.

**Promotion** to `canonical-spec` still requires a gate for every numbered
clause and atomic registry + spec + README pointer + gates updates
([DOM-6] / `writing-specs.md`). Once canonical, **edit the product spec in
place**; do not de-promote product sections the way plans are coalesced.
Unreleased promotion mistakes: git revert.

## Process and agent guidance

As of 2026-07-16 this repository carries the agent-guidance operating
model (adopted from agent-guidance @ `fc23eae`): shared agent context in
`docs/agent-context/` (follow its machine index for the read order), process
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
