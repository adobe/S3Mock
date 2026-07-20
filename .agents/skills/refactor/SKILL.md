---
name: refactor
description: Improve code readability, enforce idiomatic Kotlin conventions, and enhance documentation quality in S3Mock without changing behavior. Use when asked to clean up code, improve naming, add KDoc, or align with Kotlin/project idioms. Also invoked by the implement and test skills before adding new code to messy areas.
---

# Refactor Skill

## Entry Criteria

Use to clean up code structure/naming, improve or add KDoc, remove anti-patterns, or prepare code before a feature addition (invoked by the `implement`/`test` skill).

**Hard constraint**: must not change observable behavior. If a change would alter behavior, it belongs in the `implement` skill instead.

## Before Starting

1. Read root `AGENTS.md` and the relevant module `AGENTS.md`.
2. Read callers and tests to understand the code's purpose before making any changes.
3. Confirm existing test coverage is sufficient to catch regressions.

## Execution Steps

1. Apply naming, comment, KDoc, and idiom fixes per **[docs/KOTLIN.md](../../../docs/KOTLIN.md)**.
2. Run `make test` and/or `make integration-tests` to verify no behavior changed.
3. Update the copyright year in every modified file (`INVARIANTS.md`).
4. Invoke the **`lint` skill**.

## Completion Criteria

- [ ] No behavior changes — tests still pass
- [ ] Follows `docs/KOTLIN.md` naming, comment, and idiom conventions
- [ ] Copyright year updated in every modified file
- [ ] Lint gates pass (via `lint` skill)

## Resources

- [`AGENTS.md`](../../../AGENTS.md), [`INVARIANTS.md`](../../../INVARIANTS.md)
- Relevant module `AGENTS.md`
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md)
