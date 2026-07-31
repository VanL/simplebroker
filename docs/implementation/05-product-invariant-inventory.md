# Product Invariant Inventory

Find/map inventory of product concern families after the first CLI and
delivery vertical promotions. **Not normative.** Remaining promotions are
phase-gated Class 5 deltas in
`docs/plans/2026-07-30-product-documentation-cutover-plan.md`.

Authority machine: `docs/specs/product-section-registry.md`.
Conceptual account: `docs/program-theory.md` (non-normative for exact
behavior).
First vertical: `docs/specs/10-cli.md` (`canonical-spec`).
Second vertical: `docs/specs/11-delivery.md` (`canonical-spec`).
Third vertical: `docs/specs/12-broadcast.md` (`canonical-spec`).
Fourth vertical: `docs/specs/13-message-identity.md`
(`canonical-spec`).
The retired promotion history is source-pinned under Related Plans below.

| Family | Claim summary | Locus | Proposed codes | Known tests / notes | State |
|--------|---------------|-------|----------------|---------------------|-------|
| Delivery / claim / peek-stream | Consume claim-before-process; preferred move-to-inflight; no delete-while-peek-stream; generators thread-affine | `docs/specs/11-delivery.md`; README and agent-kernel restatements | `SB-DELIVERY-*` | `tests/test_delivery_contract_sb_delivery.py`; delivery CLI/API suites; backend finalization probes | `canonical-spec` |
| Broadcast selection and atomicity | All/pattern/exact selection; Python-only exact-target creation; backend-specific atomicity and compatibility | `docs/specs/12-broadcast.md`; README and agent-kernel restatements | `SB-BCAST-*` | `tests/test_broadcast_contract_sb_bcast.py`; shared broadcast API/CLI suites; PostgreSQL and Redis atomicity suites | `canonical-spec` |
| Message identity, allocation, exact-ID handling, and preservation | Hybrid ID representation/allocation; zero reserved for lower-bound origin while legacy selectors stay broad; ordinary-write allocation/insertion visibility; write returns; broker-global high-water/cache meaning; exact-ID insertion; move preserves ID | `docs/specs/13-message-identity.md`; `docs/implementation/08-message-identity-and-write-visibility.md`; README and agent-kernel restatements | `SB-ID-*` | `tests/test_message_identity_contract_sb_id.py`; shared reserved-zero, timestamp, write-return, SQL transaction, exact-ID, insert, cache, and move suites; real-Valkey stale-fence, visibility, monotone-resync, and `SM-REDIS-WRITE` suites | `canonical-spec` |
| Ordered timestamp selection and filter consequences | Strict open bounds as filters; late older ids under a bound; watch progress | `14-timestamp-selection.md` `[SB-SELECT-*]`; CLI string forms `[SB-CLI-5]` | `SB-SELECT-*` | `tests/test_timestamp_selection_contract_sb_select.py`; `tests/test_after_flag.py` | `canonical-spec` |
| Dump/load / claimed | Dump/load v1 format; pending-only dump; fresh load; claimed inspection | `15-persistence-io.md` `[SB-IO-*]` | `SB-IO-*` | `tests/test_persistence_io_contract_sb_io.py`; `tests/test_dump_load.py` | `canonical-spec` |
| Embedding targets/backends | Optional backends, sidecar access, packaging boundaries | README Embedding / Advanced | `SB-EMBED-*` | Extension and backend suites; separate program | `readme-only` |
| Base queue/broker operations | Remaining command/API catalog and base operation meanings, excluding broadcast and the other promoted/specialized rows above | README Command Reference / Python API | `SB-OPS-*` | Command and public API suites; later cutover phase exact delta required | `readme-only` |

## Related Plans

- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-27-product-docs-source-ownership-decision — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-28-delivery-contract-spec-promotion-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`
- `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
