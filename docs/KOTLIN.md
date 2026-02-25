# Kotlin Guidelines — S3Mock

Canonical reference for Kotlin idioms, naming conventions, and code quality standards used across this project.

## Idioms

### Null Safety
- Use `?.`, `?:`, and nullable types instead of explicit null checks
- `x?.foo()` over `if (x != null) { x.foo() }`
- `x ?: throw ...` or `requireNotNull(x)` over `if (x == null) throw ...`

### Immutability
- Prefer `val` over `var`, especially for public API properties

### Expression Bodies
- Use for single-expression functions: `fun foo() = bar()`

### Lambda Parameters
- Always name `it` in nested or non-trivial lambdas to avoid shadowing
- `.map { part -> ... }` instead of `.map { it.name }`

### `when` Expressions
- Prefer `when` over `if-else` chains with 3+ branches

### Scope Functions
- Use `.let`/`.also` when they improve readability, not gratuitously
- Use early returns to flatten deeply nested code
- Extract functions: break up methods longer than ~30 lines

### Collections
- `list.isEmpty()` / `list.isNotEmpty()` over `list.size == 0` / `list.size > 0`

### String Templates
- Use `"$value"` over `"" + value` concatenation

### Kotlin Stdlib
- Prefer Kotlin stdlib / JDK APIs over adding new third-party libraries (no Apache Commons)

## Common Anti-Patterns

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

## Naming Conventions

- **Booleans**: `is-`/`has-`/`should-`/`can-` prefixes
- **Collections**: plural nouns
- **Functions**: verb phrases (`verifyBucketExists`, `resolveVersionId`)
- **Avoid abbreviations**: `bucketMetadata` not `bktMd`

## Test Naming

- **Backtick names**: Use descriptive sentences — `` fun `should create bucket successfully`() ``
- **Legacy names**: Refactor `testSomething` camelCase names to backtick style when touching existing tests
- **Visibility**: Mark test classes as `internal`

## KDoc

- Use `/** */` for public APIs; `//` inline comments for rationale
- Comments explain **why**, never **what** — remove comments that restate the code
- Add comments for edge cases, non-obvious S3 semantics, or workarounds
- Link to AWS API docs or GitHub issues where relevant
