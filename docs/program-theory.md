# SimpleBroker Program Theory

Status: Active
Owner: SimpleBroker product owner
Boundary: Conceptual identity and design judgment, not exact current behavior.
Verification: Structural contract plus owner and independent semantic review.
Required action: Read before product-scope or design decisions; conform or
propose an explicit class-5 theory revision.
Governing process: [DOM-16]

## What “program theory” means [THEORY-0]

“Theory” here is adapted from Peter Naur's “Programming as Theory Building.”
It is not a formal theory or another name for requirements. It is the working
explanatory model that connects the problem SimpleBroker addresses to the
concepts, ownership boundaries, and conceptual constraints used to address
it. Concrete architectural and mechanical choices, and the rationale for the
current realization, belong to implementation documentation.

Naur describes the object more concretely as “a theory of how certain affairs
of the world will be handled by, or supported by, a computer program.”[^naur]
Possession is practical, not mnemonic. Someone can use the theory when they
can connect problem-world affairs to program shape, explain and justify why
that shape exists, and incorporate a new demand without losing coherence. In
repository work, that includes predicting where a proposal belongs and what
it affects and diagnosing evidence that does not fit. Memorizing this document
is not enough.

This file is the current best externalized account and a transfer surface. The
fuller working theory is reconstructed and challenged through this account
together with product contracts, implementation rationale, code, tests,
recorded alternatives, and concrete surprises.

Knuth's literate programming supplies a related representational discipline:
program explanation should be addressed to human understanding and kept in
contact with executable realization.[^knuth] SimpleBroker does not use one
literate master source. Theory, contracts, implementation rationale, code, and
tests remain separate because they own different questions; traceability and
executable gates keep them from becoming independent stories.

| Surface | Question it answers |
|---------|---------------------|
| Program theory | What problem and model make the program coherent; why do the concepts and boundaries exist? |
| Winning product contract | What exact behavior is intended now? |
| Implementation rationale | Which concrete architectural and mechanical choices realize the model, and why were they chosen? |
| Code and tests | How is the behavior realized, and what evidence fires against concrete claims? |
| Plans and alternative records | What change or competing account was considered, under which evidence and premises? |

The governing process is
[`[DOM-16]`](specs/01-development-documentation-operating-model.md#16-program-theory-and-negative-knowledge-dom-16).

[^naur]: Peter Naur, “Programming as Theory Building” (1985), “The Theory to
    Be Built by the Programmer”; reprinted in *Computing: A Human Activity*
    (1992).
[^knuth]: Donald E. Knuth,
    [“Literate Programming”](https://doi.org/10.1093/comjnl/27.2.97),
    *The Computer Journal* 27(2) (1984), 97–111; see also
    [the author's overview](https://cs.stanford.edu/~knuth/lp.html).

## Purpose and desired feel [THEORY-1]

SimpleBroker is a durable queue primitive for the space between shell pipes
and broker platforms. It should feel like a small Unix tool and a matching
Python capability: easy to start, explicit under failure, composable, and
usable without operating queue infrastructure.

“Simple” describes the use surface and operational model. Internal complexity
is justified when it protects a smaller, predictable external model and
remains locally debuggable.

## Whole-program mental model [THEORY-2]

Cooperating processes exchange durable messages through named queues on one
resolved broker target. SimpleBroker owns the semantics and coordination of
queue operations.

A backend supplies persistence and atomic storage operations. SQLite is the
local default. Optional services can supply shared storage while owning their
service topology, replication, consistency and availability mechanisms, and
recovery. The registered embedding concern and
[embedding guide](guides/python.md#embedding-simplebroker-in-your-project)
own the exact current capability statements.

The application owns message meaning, task execution, business retries,
worker topology, orchestration, and business-level completion. SimpleBroker
supplies queue primitives; it does not become the application runtime.

## Core concepts and ownership [THEORY-3]

These concepts explain the model. The linked registry concern and its winning
owner define exact current behavior.

| Concept | Conceptual meaning | Owner | Exact current contract owner |
|---------|--------------------|-------|------------------------------|
| Broker target | One resolved queue namespace and backend configuration | SimpleBroker for resolution; backend for substrate | Registry `Python library / embedding API surfaces` → [`[SB-API-*]`](specs/16-python-library-api.md) |
| Queue | Named durable message collection and operation surface | SimpleBroker | Registry `Queue and broker residual operations` → [`[SB-OPS-*]`](specs/17-ops.md); Registry `Broadcast selection, creation, and atomicity` → [`[SB-BCAST-*]`](specs/12-broadcast.md); delivery/identity/library surfaces remain with their rows |
| Message identity | Identity used to preserve and select queue messages | SimpleBroker | Registry `Message identity, allocation, exact-ID handling, and preservation` → [`[SB-ID-*]`](specs/13-message-identity.md); Registry `Ordered timestamp selection and filter consequences` → [`[SB-SELECT-*]`](specs/14-timestamp-selection.md) |
| Claim | Delivery-state transition distinct from proof of application completion | SimpleBroker | Registry `Delivery guarantees, claim/peek/watch safety` → [`[SB-DELIVERY-*]`](specs/11-delivery.md) |
| Move | Queue-level reservation or routing primitive | SimpleBroker | Registry `Delivery guarantees, claim/peek/watch safety` → [`[SB-DELIVERY-3]`](specs/11-delivery.md); Registry `Message identity, allocation, exact-ID handling, and preservation` → [`[SB-ID-5]`](specs/13-message-identity.md); Registry `Ordered timestamp selection and filter consequences` → [`[SB-SELECT-*]`](specs/14-timestamp-selection.md) |
| Watcher/waiter | Adapter from queue activity to bounded waiting or consumption | SimpleBroker | Registry `Delivery guarantees, claim/peek/watch safety` → [`[SB-DELIVERY-*]`](specs/11-delivery.md); modes remain in [README Real-time Queue Watching](../README.md#real-time-queue-watching) |
| Process session | Process-local owner of reusable backend resources | SimpleBroker | Registry `Python library / embedding API surfaces` → [`[SB-API-*]`](specs/16-python-library-api.md); rationale in [process-session ownership](implementation/06-process-session-core-ownership.md) |
| Broker core | Queue-operation protocol and shared semantics over one resolved target | SimpleBroker | Registry `Queue and broker residual operations` → [`[SB-OPS-*]`](specs/17-ops.md); Registry `Broadcast selection, creation, and atomicity` → [`[SB-BCAST-*]`](specs/12-broadcast.md); specialized identity and delivery contracts remain with their registered rows |
| Backend adapter/runner | Storage-specific atomic realization and substrate-resource ownership | Backend implementation | Registry `Python library / embedding API surfaces` → [`[SB-API-11]`](specs/16-python-library-api.md); [Python guide extensions](guides/python.md#advanced-custom-extensions) |

## Design principles [THEORY-4]

| Principle | Design consequence | Current contract |
|-----------|--------------------|------------------|
| Local-first, infrastructure-optional | The default remains operationally small; optional substrates may widen topology without redefining the core product. | Registry `Python library / embedding API surfaces` → [`[SB-API-*]`](specs/16-python-library-api.md) |
| Unix composability | CLI decisions protect composition and truthful machine use. | Registry `CLI exit codes and CLI I/O contract` → [`[SB-CLI-*]`](specs/10-cli.md) |
| Matching queue semantics across surfaces | CLI and Python express one queue model even when packaging differs. | Registry `Queue and broker residual operations` → [`[SB-OPS-*]`](specs/17-ops.md); Registry `CLI exit codes and CLI I/O contract` → [`[SB-CLI-*]`](specs/10-cli.md); Registry `Python library / embedding API surfaces` → [`[SB-API-*]`](specs/16-python-library-api.md) |
| Queue semantics, not application execution | Reusable queue primitives belong here; business workflows and task interpretation belong to consumers such as Weft. | Registry `Queue and broker residual operations` → [`[SB-OPS-*]`](specs/17-ops.md); the consumer boundary remains conceptual theory |
| Explicit safety over magical recovery | Guarantees are named narrowly enough that convenience cannot imply stronger recovery than exists. | Registry `Delivery guarantees, claim/peek/watch safety` → [`[SB-DELIVERY-*]`](specs/11-delivery.md) |
| Small concept count over small source count | Cohesive code may be large when splitting would obscure ownership or failure order. New frameworks and parallel paths need stronger cause. | [Implementation index](implementation/00-implementation-index.md) and the owning implementation document |
| Concrete pressure justifies growth | A use case, bug, or invariant supports new concepts; speculative platform growth does not. | `[DOM-16]`, active-plan evidence, and `[REV-*]` records |

## What SimpleBroker is not [THEORY-5]

SimpleBroker is not a broker fleet, managed queue service, replicated event
stream, pub/sub platform, distributed task framework, application
orchestration system, or distributed control plane. It does not own cluster
membership, leader election, storage partitioning or replication, execution
routing, or application task semantics.

This is an ownership boundary, not a claim that SimpleBroker avoids
distributed-systems problems or can only be used on one host. Cooperating
processes are a distributed system. Optional shared backends may support
clients on multiple hosts. SimpleBroker still owns queue-operation semantics;
the backend owns its distributed substrate; the application owns work
execution.

### [ALT-THEORY-001] Use ownership, not host count, as the topology boundary

Disposition: adopted
Owner: SimpleBroker product owner
Governs: [THEORY-2] and [THEORY-5]
Source record: [ALT-PT20260729-005] in docs/plans/2026-07-29-program-theory-and-negative-knowledge-plan.md
Candidate: State that SimpleBroker is categorically not for distributed systems.
Why plausible: The local SQLite default and small-tool identity make a host boundary easy to state and initially matched the product's implementation.
Evidence:
- contemporaneous: current README multi-host guidance and optional backend documentation
- owner-recalled: the approved ownership boundary in the source plan
Reason: Cooperating processes already raise distributed-systems issues, and optional PostgreSQL and Redis backends support cross-host clients. The durable boundary is which layer owns queue semantics, substrate topology, and work execution.
Current consequence: Do not use host count or the presence of a networked backend as a proxy for whether a proposal belongs in SimpleBroker.
Reconsider when: SimpleBroker takes ownership of cluster membership, substrate replication or failover, execution routing, or application task semantics.
Promoted to: none

## Tensions and falsifiers [THEORY-6]

- “Simple at the use surface” has replaced source-line count as the useful
  simplicity test. Whether the advanced surface remains progressive is a live
  tension.
- **Possession probes:** this account earns its keep only if the owner can
  place a hypothetical feature, refuse a category error, and predict the
  class of bug an audit will find before it reports. Run one such probe per
  release or per class-5 plan completion — posed to an agent or
  self-administered — and record the outcome in the plan or lessons. If
  probes start failing, the account or its possession has drifted; one
  probe, not a battery.
- Optional networked backends widen operational reach without changing the
  local-first identity. Documentation that collapses those modes would
  challenge the account.
- Repeated consumer use of private internals is evidence that a public queue
  primitive or ownership boundary may be missing.
- Repeated proposals requiring daemon ownership, membership, replication,
  application task models, or hidden workflow state challenge either the
  proposal's placement or the theory itself.
- Failures that cannot be diagnosed from the owning queue operation are
  evidence that decomposition or public error semantics need revision.

## Founding continuity and evolution [THEORY-7]

`[THEORY-1]` through `[THEORY-6]` are the current account and govern design
judgment. The quotations below are historical evidence from the initial README
at `f1bd821640d2f51006eec321b21d5341b0175cdc`. They are not independent
requirements. Each appears only to show what the current theory maintained and
how it evolved.

| Current principle | Founding evidence | Source locus | Line | Maintained | Evolved |
|-------------------|-------------------|--------------|-----:|------------|---------|
| Simple at the use surface; internal complexity must remain coherent and debuggable. | “simple enough to understand in an afternoon, yet powerful enough for real work” | `# SimpleBroker, introductory paragraph` | 12 | Low cognitive and operational burden remain design goals. | The useful unit is now the public model and local ownership, not comprehension of every source line. |
| Unix composability and queue semantics remain central. | “do one thing well” | `## Design Philosophy` | 166 | Pipes, scripts, explicit failure, and a focused queue role remain. | The product gained a Python embedding surface and optional backends without taking ownership of application execution. |
| SimpleBroker is a queue primitive, not a broker platform. | “It's not trying to replace RabbitMQ or Redis” | `## Design Philosophy` | 166 | It still does not own broker-cluster infrastructure, pub/sub, or an application control plane. | Redis can now serve as an optional backend, and SimpleBroker handles distributed-systems problems and cross-host queue coordination without becoming that platform. |
| Small concept count matters more than small source count. | “the entire codebase should stay under 1000 lines” | `## Contributing, item 1` | 239 | Simplicity remains a hard design constraint. | A line ceiling was rejected because cohesive concurrency, lifecycle, and backend code can protect a simpler external model. |

## Revisions and decision cases [THEORY-8]

Revision records put the current account first. Historical material is
evidence about why it changed, not a second theory that competes with this
one.

### [REV-THEORY-001] Simplicity moved from line count to coherent use surface

Current account: “Simple” means a small, predictable use surface and concept set. Cohesive internal complexity is justified when it protects that external model and remains locally debuggable; source-line count is not the governing measure.
Supersedes: The founding account tied simplicity partly to afternoon comprehension of the source and an explicit repository-wide limit of 1,000 lines.
Pressure: The implementation grew through real use, optional backends, and safety work. Later quality work chose cohesion, debugging locality, and auditable complexity over mechanical size limits.
Evidence:
- contemporaneous: [THEORY-7] source-pinned founding evidence, lines 12 and 239
- contemporaneous: commit `8b36b81200d9a09aa7b4710fbca15f389b0ce005`, whose README diff replaces the 1,000-line ceiling with afternoon understandability
- contemporaneous: `docs/agent-context/engineering-principles.md` §14 and `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md` at `2daa2fb48dd478fee5c01bec86add53793d55940`
- owner-recalled: approved [THEORY-1] and [THEORY-4] account in the related plan

### [REV-THEORY-002] Topology widened without becoming a broker platform

Current account: SimpleBroker remains SQLite-first and infrastructure-optional. Optional PostgreSQL and Redis/Valkey substrates may coordinate queue operations for clients on multiple hosts. SimpleBroker owns queue-operation semantics; the backend owns service topology, replication, consistency, availability, and recovery; the application owns work execution.
Supersedes: The founding account described a zero-configuration SQLite queue and excluded being a distributed message broker. A later README summary compressed that boundary into the overbroad phrase “Not for: Distributed systems.”
Pressure: PostgreSQL and Redis backends shipped, and current guidance directs multi-host users to them. Host count can no longer express the product boundary; ownership does.
Evidence:
- contemporaneous: [THEORY-7] source-pinned founding evidence and current README shared-storage guidance
- contemporaneous: `CHANGELOG.md` sections `3.0.0` and `3.7.0` at `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: source-pinned `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-05-14-simplebroker-redis-second-backend-plan.md`
- owner-recalled: the product-owner boundary recorded in the related plan

### [REV-THEORY-003] Delivery state is not application completion

Current account: Queue delivery state and successful application processing are different concepts. SimpleBroker owns queue-level transitions; the application owns the meaning of successful work and its business retry policy. The exact claim, reservation, generator, and failure-window behavior belongs to the live [`[SB-DELIVERY-*]` contract](specs/11-delivery.md).
Supersedes: The founding account said messages were “delivered exactly once using atomic DELETE operations” without distinguishing broker delivery from successful application work.
Pressure: Claim-based deletion, watchers, concurrent consumers, move reservation, and retry-on-stop generators exposed distinct loss and duplicate windows. The unqualified wording could be read as crash-safe exactly-once processing.
Evidence:
- contemporaneous: [THEORY-7] source-pinned founding README and its delivery wording
- contemporaneous: source-pinned `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-07-28-delivery-contract-spec-promotion-plan.md`
- contemporaneous: `docs/specs/11-delivery.md` `[SB-DELIVERY-1]` through `[SB-DELIVERY-5]` at `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: `CHANGELOG.md` `5.6.1` Documented entry

### [REV-THEORY-004] Queue handles do not each own a backend stack

Current account: A `Queue` is a named capability over one resolved broker target, not the conceptual owner of a backend stack. Resource lifecycle belongs to the process-session and backend boundaries. The exact sharing and isolation mechanics belong to their implementation rationale.
Supersedes: Each persistent `Queue` previously owned an independent `DBConnection`, allowing queue count to scale backend runner and pool allocation.
Pressure: Weft's multi-queue watcher naturally created one persistent handle per queue. With PostgreSQL, queue fan-out became pool fan-out and could exhaust server connection limits.
Evidence:
- contemporaneous: source-pinned `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-05-04-process-local-broker-session-plan.md`
- contemporaneous: `CHANGELOG.md` section `3.3.0` and implementation commit `9d455e7830eb77a985cbbf0b5ae7dd50811431ff`
- contemporaneous: current README Embedding and process-session guidance
- contemporaneous: `docs/implementation/06-process-session-core-ownership.md` at `2daa2fb48dd478fee5c01bec86add53793d55940`

### [REV-THEORY-005] Suspended operations retain their ownership context

Current account: A suspended operation retains the ownership context needed to settle it. Cleanup must not silently transfer that ownership or claim recovery across an incompatible execution context. Exact failure handling and recovery consequences belong to the winning delivery contract and implementation rationale.
Supersedes: Earlier recovery reasoning treated finalization as ordinary cleanup without making the suspended operation's continuing ownership explicit.
Pressure: A concrete cross-thread generator-finalization bug exposed ownership that outlived the apparent call boundary and could not be coherently settled from another thread.
Evidence:
- contemporaneous: source-pinned `197629e2c46edd755c66b272d387c08e984bf32b:docs/plans/2026-07-27-cross-thread-generator-orphan-healing-plan.md`
- contemporaneous: `docs/implementation/04-cross-thread-finalization-poisoning.md` at `2daa2fb48dd478fee5c01bec86add53793d55940`
- contemporaneous: `docs/specs/11-delivery.md` `[SB-DELIVERY-6]`
- contemporaneous: `CHANGELOG.md` section `5.5.0` and implementation commit `9d03e77d258127acfff4352435251e892daa8493`

## Related plan

- [Program theory and negative knowledge plan](plans/2026-07-29-program-theory-and-negative-knowledge-plan.md)
