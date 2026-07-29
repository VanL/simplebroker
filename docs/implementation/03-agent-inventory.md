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

Last refreshed: 2026-07-29 (Grok remained live and read-only during a
class-4 plan review, but exhausted repeated bounded turns without returning
the requested verdict; Claude Sonnet completed the fallback review in two
rounds and returned a final PASS)

| Agent family | Status | Notes |
|--------------|--------|-------|
| Claude | live | harness-level containment probed 2026-07-14; completed a two-round class-4 plan review on 2026-07-29 |
| Codex | live | OS-enforced read-only sandbox; probed 2026-07-14 |
| Grok | live, degraded | Read-only sandbox held on 2026-07-29, but a long plan review inspected sources without producing a final verdict after bounded follow-ups; not currently suitable as the sole gate |
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
