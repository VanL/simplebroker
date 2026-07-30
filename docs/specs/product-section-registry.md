# Product Section Registry

Mechanical authority table for product documentation. **One row per
concern family.** States: `readme-only` | `draft-spec` | `canonical-spec`.
The ownership rules live in `docs/README.md` and this registry.

| Concern | State | Spec section | README anchor / locus | Gate (obligation → impl → test) |
|---------|-------|--------------|----------------------|----------------------------------|
| CLI exit codes and CLI I/O contract | `canonical-spec` | `10-cli-contract.md` `[SB-CLI-1]`…`[SB-CLI-4]` | `### Exit Codes` (+ kernel Exit codes) | `tests/test_documented_exit_codes.py` (SB-CLI-1 + README link); `tests/test_agent_kernel_contract.py` (SB-CLI-1 + kernel link); `tests/test_cli_contract_sb_cli.py` (SB-CLI-2, SB-CLI-3, SB-CLI-4 behavioral binds) |
| Delivery guarantees, claim/peek/watch safety | `canonical-spec` | `11-delivery-contract.md` `[SB-DELIVERY-1]`…`[SB-DELIVERY-7]` | README Critical Safety / Delivery; agent-kernel Delivery | `tests/test_delivery_contract_sb_delivery.py` (SB-DELIVERY-1…7 + registry/README/kernel binds); `tests/test_cross_thread_finalization_poisoning.py` + backend probe suites (SB-DELIVERY-6); `tests/test_cli_broken_pipe.py` (SB-DELIVERY-7) |
| Message identity (hybrid ts, last_ts, move+checkpoint) | `readme-only` | — | README Core Concepts / agent-kernel Message IDs | (future) |
| Dump/load and claimed-row I/O | `readme-only` | — | README dump/load | (future) |
| Embedding targets, backends, sidecar | `readme-only` | — | README Embedding / Advanced | (future) |
| Base queue/broker operation catalog residual | `readme-only` | — | README Command Reference / Python API | (future) |

The base operation row owns only the remaining command/API catalog and base
operation meanings. It excludes CLI I/O and exits; delivery,
claim/peek/watch safety; message identity and move/checkpoint rules; dump/load;
and embedding, backends, and sidecar. Those concerns remain with their
existing rows.

## Transition rule

A **migration** state change requires one PR that updates this table, the
spec file (if any), the README pointer when entering `canonical-spec`, and
every Gate cell named for that row. **Entering `canonical-spec` requires a
firing test per numbered clause** (no unbound obligations). After
canonical, **edit the spec in place** for behavior/wording changes; update
this registry only when ownership or gates change (e.g. new clause + new
gate), not to “retire” the section. Incomplete migration transitions are
forbidden. Abandoning an **unshipped** `draft-spec` may return to
`readme-only` per the ownership decision.

## Related Plans

- retired: 2026-07-27-product-docs-source-ownership-decision — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md`
