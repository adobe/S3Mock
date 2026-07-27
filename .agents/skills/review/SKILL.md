---
name: review
description: Review code changes in pull requests or local diffs for S3Mock. Use when asked to review a PR, inspect changes, or provide structured feedback on correctness, conventions, test quality, and documentation. Outputs findings structured as Must fix / Should fix / Nit.
---

# Code Review Skill

## Entry Criteria

Use to review a pull request, a local diff/branch, or to provide structured code-quality feedback.

## Before Starting

Read root `AGENTS.md` and the relevant module `AGENTS.md`.

## Review Scope

Evaluate in this order, citing the specific source rule for each finding:

1. **Correctness** — does the code do what it claims (edge cases, error paths)? Check against `INVARIANTS.md` (XML naming, `S3Exception` usage, layering).
2. **Convention violations** — any `INVARIANTS.md` violation is **Must fix**. Also check the "Common Anti-Patterns" tables in `docs/SPRING.md` and `docs/KOTLIN.md` for Spring/Kotlin-specific violations (DI style, testing style, `var` on public API, etc.).
3. **Test quality** — coverage of new/changed paths, correct base class and conventions per `docs/TESTING.md`.
4. **Kotlin idioms** — per `docs/KOTLIN.md`.
5. **Documentation** — `CHANGELOG.md`/`README.md`/KDoc updated per the `document` skill's scenario table.

## Output Format

- **Must fix** — blocks merge: correctness issues, `INVARIANTS.md` violations, missing tests
- **Should fix** — idiom/doc improvements; suggest the `refactor` or `lint` skill for pure style fixes rather than asking the author to add more code
- **Nit** — optional style suggestions

## Execution Steps

1. Read root + relevant module `AGENTS.md`.
2. Evaluate all five categories above, in order.
3. Produce Must fix / Should fix / Nit findings with file/line references, each citing its source rule.

## Completion Criteria

- [ ] All five categories evaluated
- [ ] Must fix / Should fix / Nit structure used, each finding cites its source rule
- [ ] CI gate readiness assessed (ktlint, Checkstyle, tests, Docker build)
- [ ] `CHANGELOG.md` check confirmed

## Resources

- [`AGENTS.md`](../../../AGENTS.md), [`INVARIANTS.md`](../../../INVARIANTS.md)
- Relevant module `AGENTS.md`
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md), [`docs/SPRING.md`](../../../docs/SPRING.md), [`docs/TESTING.md`](../../../docs/TESTING.md)
