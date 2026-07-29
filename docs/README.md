# SimpleBroker Docs

Design, planning, and specification record for SimpleBroker.

## Product documentation ownership

Product behavior is governed by the **layered source-of-truth system**
registered in `docs/specs/product-section-registry.md`:

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
