---
name: refactor
description: Improve code readability, enforce idiomatic Kotlin conventions, and enhance documentation quality in S3Mock without changing behavior. Use when asked to clean up code, improve naming, add KDoc, or align with Kotlin/project idioms. Also invoked by the implement and test skills before adding new code to messy areas.
---

# Refactor Skill

## Entry Criteria

Use this skill when asked to:
- Clean up code structure or naming
- Improve or add KDoc and inline comments
- Remove anti-patterns identified in `docs/KOTLIN.md`
- Prepare code before a feature addition (invoked by the `implement` or `test` skill)

**Hard constraint**: Refactoring must not change observable behavior. If a change would alter behavior, it belongs in the `implement` skill, not here.

## Before Starting

1. Read `AGENTS.md` (root) and the relevant module `AGENTS.md`.
2. Read callers and tests to understand the code's purpose before making any changes.
3. Confirm that existing test coverage is sufficient to catch regressions.

## Guiding Principles

### Comments explain "why", not "what"

Remove comments that restate what the code does. Add comments only for:
- Why a decision was made
- Non-obvious S3 semantics or edge cases
- References to AWS API docs or GitHub issues that explain a workaround

Use KDoc (`/** */`) for public APIs; inline `//` comments for rationale only.

### Meaningful naming over comments

If you need a comment to explain what code does, rename instead. See [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) for naming conventions.

### Idiomatic Kotlin

Apply idioms and anti-pattern fixes from [`docs/KOTLIN.md`](../../../docs/KOTLIN.md): null safety, expression bodies, scope functions, `isEmpty()`/`isNotEmpty()`, `when` expressions, named `it` alternatives.

### KDoc for public APIs

Document what, why, and gotchas. Link to [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) where relevant.

## Execution Steps

1. Read callers and tests first; confirm no behavior changes are intended.
2. Apply changes: naming, comments, KDoc, idiomatic Kotlin.
3. Run tests to verify no behavior changed: `make test` and/or `make integration-tests`.
4. Update the copyright year in every file you modify — see `INVARIANTS.md`.
5. Invoke the **`lint` skill** to fix formatting and verify style gates pass.

## Completion Criteria

- [ ] No behavior changes — tests still pass
- [ ] Naming is self-documenting; no "what" comments remain
- [ ] Comments explain "why"
- [ ] Public APIs have KDoc
- [ ] Copyright year updated in every modified file
- [ ] Lint gates pass (via `lint` skill)

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — DO/DON'T, Code Style section
- [`INVARIANTS.md`](../../../INVARIANTS.md) — hard constraints (copyright year rule)
- Relevant module `AGENTS.md`
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) — Kotlin idioms, naming conventions, anti-patterns, KDoc guidelines
