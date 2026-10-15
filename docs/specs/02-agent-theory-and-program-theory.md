# Agent Theory and Program Theory

Status: Reference — definitional primer
Owner: agent-theory maintainers (definitions); product owners (their own
`docs/program-theory.md` account)
Boundary: Explains what **Agent Theory** and **program theory** mean, how they
differ from behavioral specs and process rules, and where to read next. This
file is a reference primer — it is **not** the program theory of any product,
**not** a behavioral contract, and **not** required reading at every session
start.
Verification: Consistency with `docs/program-theory.md` and
`01-development-documentation-operating-model.md` `[DOM-2]` and `[DOM-3]`.
Required action: Read this file when the terms are unfamiliar, when onboarding
a human or agent to the workflow, or before writing a product theory. Link to
this file rather than re-explaining the definitions in chat.

The `[AT-REF-*]` codes below are citation anchors only; `[DOM-*]` owns the
normative process rules.

---

## What Agent Theory is [AT-REF-1]

**Agent Theory** is a practical discipline for building software with coding
agents while keeping humans in possession of the program's theory: what the
system is, why it has its shape, what it deliberately refuses to do, and how
changes are proved.

Its central rule is simple:

> Humans may delegate implementation, not understanding.

Agent Theory is **not** big design up front. A team's theory of a program
starts partial and sharpens feature by feature through owner dialogue,
specifications, implementation, independent review, and test evidence.

### Three names that are easy to confuse

| Term | Meaning |
|------|---------|
| **Agent Theory** | The engineering discipline (written without a hyphen in prose) |
| **The `agent-theory` hub** | The reference repository and starter kit for applying the discipline |
| **`agent-theory`** | The repository slug and command name |

### The failure mode it prevents

Coding agents can produce working code faster than humans can maintain a mental
model of the system. Without deliberate checkpoints, a codebase keeps growing
after neither the human owner nor the agents understand how the whole program
fits together. Agent Theory treats human understanding as a required output of
every change, not an accidental side effect.

### Three foundational ideas

| Author | Core takeaway for Agent Theory |
|--------|--------------------------------|
| **Peter Naur** (*Programming as Theory Building*, 1985) | Programming is the act of building a mental model of the problem and the program. Documentation helps transfer that model, but never replaces a person who holds it. |
| **Donald Knuth** (*Literate Programming*, 1984) | Explanations exist to build human understanding and must stay tied to the working code. Here, separate documents are linked by traceability checks rather than combined into one master file. |
| **Armin Ronacher** (*"The Tower Keeps Rising"*, 2026) | Coding agents remove the everyday friction that used to force teams to agree on shared concepts and boundaries. Deliberate orientation and review checkpoints replace that lost friction. |

---

## What a program theory is [AT-REF-2]

A **program theory** is the working explanatory model of a software system:
what real-world problem it solves, **what using and extending it should feel
like (its command/API grammar and the core composable operations new work must
build on rather than inventing side doors)**, what its core concepts are, who
owns each boundary, what it refuses to build, and what evidence would show the
model is wrong.

Following Peter Naur's *Programming as Theory Building* (1985), a program
theory is **not**:

- a formal mathematical proof,
- an exhaustive catalog of user requirements or flag-by-flag API schemas
  (though concrete **taste anchors**—illustrative command grammars, verb
  rhythms, and composable operations that communicate the program's **feel**—do
  belong in program theory),
- a dump of architecture diagrams, or
- a document that can substitute for human understanding.

### Holding the theory vs. writing the current account

Naur distinguishes between **holding a theory** in your head and **writing a
document** on disk. Agent Theory keeps that distinction explicit:

| Concept | What it means in practice |
|---------|---------------------------|
| **Holding the theory (possession)** | A person's practical ability to place a new feature, explain why the code is shaped the way it is, reject ideas that do not fit the system, and diagnose surprising bugs. |
| **The written account (`docs/program-theory.md`)** | A concise document in the repository that gives humans and zero-context agents the same vocabulary, idiom/feel, boundaries, and non-goals at the start of a task. |

Memorizing `docs/program-theory.md` is not holding the theory. The file gives
humans and agents a shared starting point; day-to-day work with specs, code,
tests, and unexpected bugs is what builds and refreshes the actual mental
model.

You hold the theory of a program when you can do four things:

1. Connect real-world problems to the structure of the code.
2. Explain *why* each major boundary and design choice exists.
3. Place a new feature request without breaking the existing design or idiom.
4. Diagnose unexpected behavior when the system or an agent surprises you.

A quick self-test: when the system misbehaves, do you feel *betrayed by an
invariant you know*, or *surprised by a system you do not really own*?

### Product theory vs. hub theory

Every repository has one `docs/program-theory.md`, and it always describes
**that repository**:

| Repository | What `docs/program-theory.md` describes |
|------------|-----------------------------------------|
| **A product or library repository** | That product's problem, feel/idiom, composable operations, concepts, ownership boundaries, non-goals, and falsifiers. |
| **The `agent-theory` hub repository** | The guidance system itself (how the process, documents, and gates work). |

When you install the starter kit into a product repository, the installer
writes a short `Status: Stub` file at `docs/program-theory.md`. Replace that
stub with your product's own theory before committing product behavior or
architecture. Never copy the hub's guidance theory into a product repository as
if it were the product's identity.

### Extending the model with module theory

Keep your top-level `docs/program-theory.md` short so every session can read it
in a few minutes. When a specific subsystem or package needs its own ownership
table, design rule, or local non-goals, write a **module theory** file
(typically `<module>/MODULE-THEORY.md`) right next to that code. Agents read a
module theory only when their task enters that module, not at every session
start.

---

## How program theory differs from other documents [AT-REF-3]

Each document surface in the repository answers a different question:

| Document surface | Question it answers |
|------------------|---------------------|
| **Program theory** (`docs/program-theory.md`, `MODULE-THEORY.md`) | What kind of system or module is this? What is its feel/idiom and core composable operations? Why do these concepts, boundaries, and non-goals exist? |
| **Process and operating model** (`[DOM-*]`, `docs/agent-context/`, `skills/`) | How do humans and agents classify tasks, write plans, run reviews, verify changes, and maintain docs? |
| **Product contracts** (`docs/specs/`, authoritative README sections) | What exact behavior, flags, schemas, and invariants must hold **right now**? |
| **Implementation docs** (`docs/implementation/`) | Why is the current code structured this way, and where are the key edit points? |
| **Code, tests, and gates** | How does the system run, and what automated checks prove it works? |
| **Plans and decision records** (`docs/plans/`, `[ALT-*]` / `[REV-*]`) | How was a specific change executed, and which alternatives were rejected or revised? |

Program theory **frames** how you interpret requests and where you place new
code, but it never overrides a behavioral spec. Simultaneously, specs must not
silently contradict the program theory. When a change reveals a conflict
between the theory and a spec, pause and update one of them explicitly.

This file lives in `docs/specs/` as a **reference primer** alongside the
operating-model spec (`01-development-documentation-operating-model.md`). It is
not a behavioral specification for a product.

---

## How theory grows over time [AT-REF-4]

A program theory is built iteratively alongside the software:

```text
concept → owner dialogue → draft theory → spec → implementation
        → test & runtime evidence → revised theory
```

- **Prototype freely on a stub.** While `docs/program-theory.md` is still a
  bootstrap `Stub`, you can explore and prototype, as long as you do not treat
  the stub as product authority.
- **Crystallize before committing product behavior.** Before committing
  product-scope behavior or architecture, interview the owner and write at
  least a `Draft` theory.
- **Communicate program feel ("taste up front"), not Big Design Up Front (BDUF).**
  Coding agents cannot infer aesthetic taste or ergonomic feel from abstract
  box-and-arrow diagrams alone. If a program theory states only abstract
  ownership boundaries, agents will satisfy the diagram while inventing clunky,
  inconsistent commands and side-door abstractions. A product's program theory
  should state its **intrinsic feel, command/API grammar, and core composable
  operations** up front with concrete "taste anchors" (for example: *"major
  daily verbs at the top level, `noun verb` elsewhere; here are our core
  operations—compose from these rather than inventing new primitives"*). What
  separates **taste up front** from **BDUF** is *what* gets fixed before code
  exists: fix the **problem, trust boundaries, composable operations, and
  ergonomic feel** in program theory, while proving internal storage schemas,
  file formats, and implementation machinery empirically through vertical
  end-to-end slices rather than building horizontal framework layers in a
  vacuum.
- **Revise when reality pushes back.** Product-scope Class 5 work requires at
  least a current `Draft` account, and that account should be updated whenever
  implementation or debugging reveals a new constraint.

---

## Where to go next [AT-REF-5]

| If you want to... | Read |
|-------------------|------|
| See how a theory document is structured | `docs/program-theory.md` |
| Write a product or module theory via owner interview | `skills/crystallize-program-theory/SKILL.md` |
| Check how agents load context at session start | `01-development-documentation-operating-model.md` (`[DOM-2]`, `[DOM-3]`) |
| Read the human overview | Root `README.md` |
| Install the starter kit into another repo | The `agent-theory` bootstrap command (`bootstrap-agent-theory`) |
| See which repositories use these practices | Root `README.md` in the `agent-theory` repository |

---

## Related Plans

- `docs/plans/2026-10-14-agent-theory-delta-wave-plan.md`
- "2026-07-30-program-theory-and-module-theory.md" in `agent-theory`
