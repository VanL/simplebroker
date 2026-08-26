# Release-gate startup-failure recovery plan

Status: active

Class: 3. Publication recovery crosses the GitHub Actions, immutable-tag,
Trusted Publishing, and GitHub Release boundaries.

## Source Documents

- `docs/agent-context/runbooks/hardening-plans.md`, especially section 15's
  immutable-identifier recovery and exact-SHA publication gates
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], and [DOM-15]
- `bin/release.py`, the executable release owner
- `.github/workflows/release-gate.yml`,
  `.github/workflows/release-gate-pg.yml`, and
  `.github/workflows/release-gate-redis.yml`, the publication owners
- `.github/scripts/require_green_workflows.py` and
  `.github/scripts/release_publication.py`, the exact-SHA and staged-release
  gates

## Incident and decision

The immutable `v7.5.0` tag was pushed only after its exact SHA passed the three
required test workflows. GitHub then created release-gate run `32984137486`
with `startup_failure`, no jobs, no logs, and no billable time. GitHub refuses
to rerun that class of run. The workflow is unchanged from the successful
`v7.4.2` gate, so repository runtime code did not cause the startup failure.

Do not move or recreate the tag. The tagged `v7.5.0` workflow does not contain
`workflow_dispatch`, so adding the trigger on `main` cannot retroactively make
that immutable workflow dispatchable. Release core as corrective version
`7.5.1`; leave `v7.5.0` as an unpublished incident tag.

For future releases, add `workflow_dispatch` to each existing
trusted-publisher workflow, but do not accept a tag as ordinary input and do
not dispatch from `main`. A future recovery command selects an immutable tag
that already contains the trigger as the workflow run ref
(`gh workflow run <workflow> --ref <tag>`). GitHub then sets `github.ref`,
`github.ref_name`, and `github.sha` from that tag, so the unchanged gate runs
from the same identity as a tag push.

This distinction is security-relevant. The live `pypi` environment permits
only `v*`, `simplebroker_pg/v*`, and `simplebroker_redis/v*` tag refs. A
dispatch from `main` would remain `refs/heads/main` even if a job later
resolved a tag, and the environment would correctly reject it. Do not widen
that policy. Normalize push and recovery through the tag ref instead.

## Invariants

1. Tag-push release remains the normal path.
2. `v7.5.0` is not dispatched, moved, or published. The corrective release is
   `v7.5.1` through the normal tag-push path.
3. Future manual recovery is dispatched only when the existing immutable tag
   itself contains the trigger. The tag is the run ref and the workflow takes
   no alternate tag input.
4. The tag ref supplies the existing `github.ref_name` and `github.sha`
   values used by every current gate.
5. Required test workflows must be green for that exact tag SHA.
6. Build, attestation, draft, PyPI publish, and final GitHub Release retain
   their existing tag and SHA inputs.
7. Push and recovery for one tag share `release-gate-${{ github.ref }}`.
   Publication runs queue rather than canceling one another, because
   cancellation after PyPI but before GitHub Release would strand a partial
   release.
8. Existing PyPI Trusted Publishing stays unchanged. No token or alternate
   authentication path or environment-policy widening is added.

## Verification and rollout

- Contract-test all three workflows for input-free manual dispatch, the
  existing tag-ref concurrency key, non-canceling publication, and continued
  use of `github.ref_name` / `github.sha`.
- Run workflow tests, release-script tests, Ruff, Mypy, ShellCheck, DOM-15,
  plan-context, and diff checks.
- Obtain an independent review of the recovery boundary.
- Commit and push the forward recovery hardening to `main`. Add a 7.5.1
  changelog entry that records the unpublished 7.5.0 tag incident, then run
  the normal exact-SHA release process for 7.5.1.
  Verify exact-SHA CI evidence, PyPI artifacts, and the immutable GitHub
  Release before continuing the ordered extension release.

## Rollback and stop gates

Before PyPI publication, a failed release leaves only its immutable tag and
any draft release. After PyPI publication there is no rollback; the workflow
must finish the matching GitHub Release. Do not dispatch `v7.5.0`, dispatch
from a branch, add a manual tag input, widen the `pypi` environment, or cancel
an in-progress publication. Stop if a future recovery tag lacks the trigger,
the run ref is not the intended tag, or any exact-SHA workflow is not green.
