---
name: refactor
description: Improve code readability, enforce idiomatic conventions, and enhance documentation quality. Use when asked to clean up code, improve naming, add meaningful comments, or align with Kotlin/project idioms.
---

# Refactor Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before making changes — especially DO/DON'T and Code Style sections. Refactoring must not change behavior — only readability and style.

## Pre-Flight

- Understand the code's purpose — read callers and tests first
- Confirm changes are purely cosmetic (no behavior changes)
- Check existing test coverage

## Guiding Principles

### Comments Explain "Why", Never "What"

Remove comments that restate code. Add comments for *why* a decision was made, edge cases, or non-obvious S3 semantics. Reference AWS S3 API docs or GitHub issues for workarounds. Use KDoc (`/** */`) for public APIs; inline comments (`//`) for rationale.

### Meaningful Naming Over Comments

If you need a comment to explain *what* code does, rename instead. See **[docs/KOTLIN.md](../../../docs/KOTLIN.md)** for naming conventions.

### Idiomatic Kotlin & Common Anti-Patterns

See **[docs/KOTLIN.md](../../../docs/KOTLIN.md)** for the full list of Kotlin idioms, common anti-patterns and their fixes, and scope function guidance.

### KDoc for Public APIs

Document what, why, and gotchas. Link to AWS API docs where relevant. See **[docs/KOTLIN.md](../../../docs/KOTLIN.md)** for KDoc conventions.

## Checklist

- [ ] No behavior changes — tests still pass
- [ ] Run `./mvnw ktlint:format`
- [ ] Comments explain *why*, not *what*
- [ ] Public APIs have KDoc
- [ ] Names are self-documenting
