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
