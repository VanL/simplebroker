# Specs Index

This directory contains the repository's source-of-truth specs for intended
behavior — **currently scoped to the development process only** (see the
scope note in `README.md` here): SimpleBroker's product behavior is
specified by the root `README.md`, not by this tree.

Use this numbered index as the canonical starting point for specs. Keep
`README.md` as a thin pointer so directory browsing and numbered read order
stay aligned instead of competing.

## Rules

- Specs define intended behavior, invariants, and verification expectations.
- Specs use stable reference codes so plans and code can cite exact
  requirements.
- Specs backlink related plans under `## Related Plans`.
- If behavior changes materially, update the spec before or with the code.

## Recommended Starting Points

1. `01-development-documentation-operating-model.md`

## Naming

- Use stable filenames.
- Numbered prefixes are recommended when the corpus is expected to grow.
- Prefer concise, descriptive titles over ticket-like names.

## Related Surfaces

- `docs/plans/` for execution
- `docs/implementation/` for rationale and repository maps
- `skills/` for reusable workflow instructions
