# Specs Index

Numbered entry point for `docs/specs/`. Directory `README.md` is a thin
pointer; this file is the read order.

Before product-scope or design judgment, read
`docs/program-theory.md`. It explains the conceptual model; the specs and
registry below remain the owners of exact intended behavior.

## Process specs

1. `01-development-documentation-operating-model.md` — `[DOM-*]`

## Product specs

Product section authority: `product-section-registry.md`.

1. `10-cli.md` — `[SB-CLI-*]` (exit codes, Unix stream roles, JSON
   shapes when the registry marks canonical)
2. `11-delivery.md` — `[SB-DELIVERY-*]` (claim, watch, move, peek,
   generators when the registry marks canonical)
3. `12-broadcast.md` — `[SB-BCAST-*]` (selection, create_missing,
   atomicity, CLI selectors when the registry marks canonical)
4. `13-message-identity.md` — `[SB-ID-*]` (IDs, allocation, exact
   forms, high-water/cache, move preservation when the registry marks
   canonical)
5. `14-timestamp-selection.md` — `[SB-SELECT-*]` (strict open bounds
   as filters; late older ids; watch progress when the registry marks
   canonical)
6. `15-persistence-io.md` — `[SB-IO-*]` (dump/load format, filters,
   fresh load, claimed-row inspection when the registry marks canonical)

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
