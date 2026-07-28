# Product Invariant Inventory

Find/map inventory of product concern families still in `readme-only`
state after the first CLI vertical promotion. **Not normative.** Next
promotions are separate Class 5 plans citing
`docs/specs/product-section-registry.md`.

Authority machine: `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`.
First vertical: `docs/specs/10-cli-contract.md` (`canonical-spec`).

| Family | Claim summary | Locus | Proposed codes | Known tests / notes | State |
|--------|---------------|-------|----------------|---------------------|-------|
| Delivery / claim / peek-stream | Consume claim-before-process; preferred move-to-inflight; no delete-while-peek-stream; generators thread-affine | README Critical Safety / Delivery; agent-kernel Delivery | `SB-DELIVERY-*` | Delivery-oriented CLI and API tests; agent-kernel contract (peek-stream forbid) | `readme-only` |
| Message identity / move+checkpoint | Hybrid timestamps as ids; `last_ts` / checkpoint patterns; move preserves identity semantics | README Core Concepts; agent-kernel Message IDs | `SB-ID-*` | Message-id / timestamp tests; move + filter suites | `readme-only` |
| Dump/load / claimed | Dump/load line formats; claimed-row inspection; vacuum reclaim | README dump/load; agent-kernel Dump/load | `SB-IO-*` | Dump/load CLI tests | `readme-only` |
| Embedding targets/backends | Optional backends, sidecar access, packaging boundaries | README Embedding / Advanced | `SB-EMBED-*` | Extension and backend suites; separate program | `readme-only` |

## Related Plans

- `docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
- `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`
