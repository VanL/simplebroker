# Agent Inventory

Status note (2026-07-16 bootstrap): this repository shares its
development environment with the agent-guidance hub; the table below is
carried from the probe record in `skills/call-agent/SKILL.md` (which
holds the exact invocations and containment caveats). Refresh via that
skill's probe procedure when tooling changes.

## Purpose and Scope

This document records which agent families are currently available in the
environment and which ones are preferred for independent review work.

Keep it lightweight and refresh it when tooling changes materially.

## Governing Spec References

- `docs/specs/01-development-documentation-operating-model.md` [DOM-3]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-11]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-13]

## Verification Method

To refresh this inventory:

1. run a small read-only review or no-op prompt against each available agent
   interface
2. record whether it is:
   - verified usable
   - present but blocked by credentials or configuration
   - present but currently failing at invocation time
3. update the refresh date and notes

## Current Observed Availability

Session refresh (2026-11-11): Claude 2.1.293 completed a real repository
review with only `Read`, `Grep`, and `Glob` exposed, no MCP servers, and a
successful native `Read`; the restricted tool surface left no repository
write path. Grok 1.0.77 completed the absolute-`--prompt-file` headless probe;
native reads succeeded and an actual repository write was denied with `EPERM`,
leaving the sentinel absent. Exact commands, completion checks, and the macOS
OrbStack caveat are recorded in `skills/call-agent/SKILL.md`.

Session refresh (2026-10-13): Claude liveness returned `PROBE-OK` with exit 0
under a 120-second bound. The read-only PostgreSQL attribution plan review
completed with exit 0 in 261.6 seconds under a 540-second bound, returning PASS
conditional on two test-design fixes. Scoped verification completed in 152.8
seconds under the same bound, returning PASS for all nine accepted fixes.
Findings and author dispositions are in retired plan
`00b837f:docs/plans/2026-10-13-postgres-connection-attribution-plan.md`; see
the ledger in `docs/plans/README.md`. Native Codex
independent review (`codex exec -s read-only ... -c 'model_reasoning_effort="high"'`
per `skills/call-agent/SKILL.md`) also returned PASS. Other families were not
re-probed. The implementation review exited 0 in 246.6 seconds under the same
540-second bound, returning no blocker with seven P3 findings; author
dispositions are in the same plan.

Last refreshed: 2026-09-13 (Claude completed the six-finding remediation plan
review and scoped verification read-only in 296.4 and 127.6 seconds, both PASS;
implementation slice review and final integrated review completed in 272.8 and
495.3 seconds with no blockers; findings and dispositions
are recorded in
`1c6898b:docs/plans/2026-09-13-verified-review-remediation-plan.md`)

| Agent family | Status | Notes |
|--------------|--------|-------|
| Claude | live, review-eligible | 2.1.293 restricted-tool review verified 2026-11-11: only `Read`, `Grep`, and `Glob`, no MCP servers, real native read succeeded; harness-level tool restriction, not an OS sandbox |
| Codex | live | OS-enforced read-only sandbox; probed 2026-07-14 |
| Grok | live, review-eligible with host caveat | 1.0.77 absolute-prompt-file probe verified 2026-11-11: native read succeeded, direct write denied with `EPERM`, sentinel absent; macOS OrbStack access remains outside the direct-write proof |
| Qwen | blocked | API 404 / paid-slug config as of 2026-07-14 |
| Kimi | probe incomplete | no headless containment mode found |
| opencode | revoked | write-attempt probe failed 2026-07-14 |
| Gemini | do not use | CLI deprecated upstream |

## Review Preference

For plan review and final review:

1. prefer a different agent family than the authoring agent
2. if several are available, prefer one that has not already shaped the plan
3. if only one family is available, note that limitation and do a stricter
   fresh-eyes review

## Refresh Guidance

Update this file when:

- the available tool surface changes
- a new agent family becomes available
- an existing agent family is removed
- review workflow preferences change materially
