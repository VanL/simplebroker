# Product Documentation Source Ownership Decision

**Date:** 2026-07-27  
**Status:** completed — decision accepted; first vertical landed with
`2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`  
**Class:** decision artifact (feeds Class 5+P implementation plan  
`2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`)  
**Owner:** repository maintainer  

## Decision

Adopt **layered product documentation ownership** with a **single mechanical
registry** and a **forward migration state machine** for moving concerns out
of the README into product specs. Once a section is `canonical-spec`, it is a
**living product spec**: update it in place per [DOM-6] / `writing-specs.md`
(and agent-guidance), do **not** “retire” or de-promote it back to the README.
Git is the archive for old wording. Do **not** keep federated truth without a
registry. Do **not** execute the multi-phase IA roadmap plan as written.

## Context

- Root `README.md` is today the sole product behavior SoT
  (`docs/README.md`, `docs/specs/README.md`).
- Density feedback and agent-context practice push toward coded invariants
  and spec↔implementation mapping.
- `docs/agent-kernel.md` and `llms.txt` are orientation surfaces, not a third
  product SoT.
- Backstitch, hosted docs, and full README restructuring are **separate**
  programs (own plans after this decision and a first vertical land).

## Ownership rules (normative once promoted)

1. **Migration states** (exactly one per product concern family, recorded in
   the product section registry). These govern **where authority lives during
   extraction from the README**, not a retirement lifecycle for product specs:

   | State | Meaning |
   |-------|---------|
   | `readme-only` | Normative text lives only in root `README.md` (and CHANGELOG deltas). Not yet extracted. |
   | `draft-spec` | Spec section exists under `docs/specs/`; **README still wins** on conflict until promotion. Scaffold / review only. Unbound obligations (spec without a firing test) are allowed only here. |
   | `canonical-spec` | Spec section is the **living** product SoT for that concern. README may restate short tables/bullets and **must** link the section code. On conflict, **spec wins**. **Every** numbered clause must have a named gate (obligation → implementation → test). |

2. **After `canonical-spec`:** **update the spec in place** when intended
   behavior or wording changes ([DOM-6], `writing-specs.md`: specs define
   intended behavior; update before or with the code; agent-guidance: docs
   change in place, git is the archive). Do **not** retire product-spec
   sections the way plans are soft-retired. Split/replace sections with stable
   codes and updated backlinks; do not invent a parallel “old SB-* retired”
   warehouse.

3. **Atomic migration transitions** (extraction only; each in one PR):

   | Transition | Required artifacts |
   |------------|-------------------|
   | `readme-only → draft-spec` | Registry row; draft spec file/section |
   | `draft-spec → canonical-spec` | Registry row; full clause set; README pointer; **gate(s) for every clause** |
   | `draft-spec → readme-only` | Only if abandoning an unshipped scaffold: registry row; drop or stop citing the draft (no public “canonical” claim ever made) |

   No silent dual authorship. **Unreleased promotion mistake:** git revert of
   that PR is fine. **Shipped canonical section needs a fix:** edit the
   canonical section (Class 5 as required), update README restatement/links
   and gates, CHANGELOG if user-visible — do **not** de-promote authority
   back to the README as the primary fix path.

4. **Agent kernel** (`docs/agent-kernel.md`) is a **view**: it may cite
   `[SB-*]` codes; it must not invent obligations absent from the winning SoT
   (readme-only or canonical-spec).

5. **CHANGELOG** continues to record user-visible behavior deltas regardless
   of section state.

6. **Process specs** (`[DOM-*]`) remain process-only; product codes use the
   `SB-*` prefix in numbered product spec files (`10+`).

7. **Plans vs specs:** plan harvest/retirement (coalescing) does **not** apply
   to product specs. Plans may be `retired-pending`; product specs stay live
   and are revised in place.

## Out of scope for this decision

- Choosing Read the Docs vs GitHub Pages  
- Installing or gating on Backstitch  
- Bulk README rewrite  
- Migrating more than the first vertical (that is the implementation plan)

## Consequences

- Implementation plan must be **Class 5+P** for doctrine + first product
  section promotion.
- Roadmap items (Backstitch, hosted docs, full README reshape) require
  **separate plans** after the first vertical proves the registry machine.

## Related Plans

- Supersedes as executable work:
  `2026-07-27-information-architecture-improvement-plan.md` (roadmap only)
- Implementation:
  `2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
