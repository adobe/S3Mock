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

If you need a comment to explain *what* code does, rename instead:
- Booleans: `is-`/`has-`/`should-`/`can-` prefixes
- Collections: plural nouns
- Functions: verb phrases (`verifyBucketExists`, `resolveVersionId`)
- Avoid abbreviations (`bucketMetadata` not `bktMd`)

### Idiomatic Kotlin

- **`.let`/`.also`**: Use when they improve readability, not gratuitously
- **Expression bodies**: For single-expression functions
- **Null safety**: `?.`, `?:` over `if (x != null)` checks
- **Named `it`**: Always name in nested or non-trivial lambdas
- **`when`**: Over `if-else` chains with 3+ branches
- **Early returns**: Flatten deeply nested code
- **Extract functions**: Break up methods longer than ~30 lines

### Common Anti-Patterns to Fix

| Anti-Pattern | Refactor To |
|---|---|
| `if (x != null) { x.foo() }` | `x?.foo()` |
| `if (x == null) throw ...` | `x ?: throw ...` or `requireNotNull(x)` |
| `list.size == 0` / `list.size > 0` | `list.isEmpty()` / `list.isNotEmpty()` |
| `"" + value` | `"$value"` |
| `Collections.emptyList()` | `emptyList()` |
| `object.equals(other)` | `object == other` |
| `!(x is Foo)` / `!(list.contains(x))` | `x !is Foo` / `x !in list` |
| `for + add` loops | `.map { ... }` |
| Empty catch blocks | At minimum, log the exception |
| Magic numbers/strings | Named constants |

### KDoc for Public APIs

Document what, why, and gotchas. Link to AWS API docs where relevant.

## Checklist

- [ ] No behavior changes — tests still pass
- [ ] Run `./mvnw ktlint:format`
- [ ] Comments explain *why*, not *what*
- [ ] Public APIs have KDoc
- [ ] Names are self-documenting
