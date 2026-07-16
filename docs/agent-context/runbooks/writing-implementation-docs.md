# Writing Implementation Docs

Implementation docs explain why the current design exists, which boundaries it
owns, and what a future editor must understand before changing it.

They complement specs; they do not replace them.

## Purpose

Use implementation docs to capture:

- current architecture rationale
- important boundaries and invariants in the implementation
- key files, modules, or data flows
- tradeoffs and rejected alternatives that still matter
- debugging and maintenance hotspots

Write them so an agent can quickly find the rationale, invariants, and likely
edit points without relying on unstated human context.

Agent-usable implementation docs should make these explicit whenever they
matter:

- owner
- boundary
- verification path
- required action for future editors

## File Placement

- Put implementation docs in `docs/implementation/`.
- Use descriptive filenames.
- Keep a `README.md` in the directory that explains the role of the folder.

## What Good Implementation Docs Do

- explain why the code is shaped this way
- name the governing spec sections
- point to the key files or modules
- call out constraints that a refactor must preserve
- help a new engineer decide where to edit

## What They Should Not Do

- duplicate the spec verbatim
- provide a line-by-line code walkthrough
- become stale inventories of every file in the repo
- replace real code comments or docstrings where code-level ownership matters

## Recommended Sections

### 1. Purpose and Scope

What slice of the implementation this document explains.

### 2. Governing Spec References

Exact spec files and reference codes that own the behavior.

### 3. Design Rationale

Why the implementation is structured the current way. Include important
tradeoffs or rejected alternatives when they still constrain future work.

### 4. Boundaries and Invariants

What must not drift even if the code is reorganized.

### 5. Key Files or Modules

Where the important logic lives today.

### 6. Change Guidance

What future editors should read first, reuse, or avoid duplicating.

### 7. Related Plans

Which plans introduced or materially changed the design.

## Maintenance Rules

- Update implementation docs when the rationale, ownership, or key boundaries
  change.
- Add or refresh repository maps or code reference maps when new modules become
  important entry points.
- Keep nearby spec backlinks aligned.
- Prefer short, durable explanations over exhaustive prose.
- If a document feels legible to a human but leaves an agent unsure where to
  edit, what governs the behavior, or what must not drift, tighten it and
  suggest the exact clarification that would help.

## Anti-Patterns

- “how it works” docs that ignore why it was designed that way
- implementation notes with no governing spec references
- stale file lists that no longer match reality
- notes that rely on shared team intuition instead of explicit boundaries
- docs that only make sense to the original author
