# Implementation Index

This directory contains implementation-facing documentation: rationale,
boundaries, repository maps, and change guidance for the current system.

Use this numbered index as the canonical starting point for implementation
docs. Keep `README.md` as a thin pointer so directory browsing and numbered
read order stay aligned instead of competing.

## Rules

- Implementation docs explain why the current design exists.
- Program theory explains the conceptual model; winning contracts own exact
  behavior. Implementation docs do not compete with either.
- Implementation docs cite governing spec sections.
- Implementation docs should help future editors decide where to read and edit.
- Implementation docs should be updated when rationale or ownership changes.

## Recommended Starting Points

1. `01-documentation-system.md`
2. `02-repository-map.md`
3. `03-agent-inventory.md`
4. `04-cross-thread-finalization-poisoning.md`
5. `05-product-invariant-inventory.md`
6. `06-process-session-core-ownership.md`
7. `07-complexity-and-state-machine-map.md`
8. `08-message-identity-and-write-visibility.md`

For product-scope or concept-ownership questions, begin with
`../program-theory.md` before selecting the implementation document.
