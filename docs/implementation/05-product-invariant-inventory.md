# Product Invariant Inventory

Find/map inventory of product concern families after the first CLI and
delivery vertical promotions. **Not normative.** Next promotions are
separate Class 5 plans citing
`docs/specs/product-section-registry.md`.

Authority machine: `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`.
First vertical: `docs/specs/10-cli-contract.md` (`canonical-spec`).
Second vertical: `docs/specs/11-delivery-contract.md` (`canonical-spec`),
promoted by
`docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`.

| Family | Claim summary | Locus | Proposed codes | Known tests / notes | State |
|--------|---------------|-------|----------------|---------------------|-------|
| Delivery / claim / peek-stream | Consume claim-before-process; preferred move-to-inflight; no delete-while-peek-stream; generators thread-affine | `docs/specs/11-delivery-contract.md`; README and agent-kernel restatements | `SB-DELIVERY-*` | `tests/test_delivery_contract_sb_delivery.py`; delivery CLI/API suites; backend finalization probes | `canonical-spec` |
| Message identity / move+checkpoint | Hybrid timestamps as ids; `last_ts` / checkpoint patterns; move preserves identity semantics | README Core Concepts; agent-kernel Message IDs | `SB-ID-*` | Message-id / timestamp tests; move + filter suites | `readme-only` |
| Dump/load / claimed | Dump/load line formats; claimed-row inspection; vacuum reclaim | README dump/load; agent-kernel Dump/load | `SB-IO-*` | Dump/load CLI tests | `readme-only` |
| Embedding targets/backends | Optional backends, sidecar access, packaging boundaries | README Embedding / Advanced | `SB-EMBED-*` | Extension and backend suites; separate program | `readme-only` |

## Related Plans

- `docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`
- `docs/plans/2026-07-27-product-docs-source-ownership-decision.md`
- `docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`
