---
name: refactor
description: Improve code readability, enforce idiomatic conventions, and enhance documentation quality. Use when asked to clean up code, improve naming, add meaningful comments, or align with Kotlin/project idioms.
---

# Refactor Skill — S3Mock

Improve readability, idiomatic style, and documentation quality in S3Mock code (Kotlin 2.3, Spring Boot 4.0.x).

## When to Use

- Improving readability of complex logic
- Replacing verbose or non-idiomatic code with Kotlin idioms
- Improving comments and documentation to explain *why*, not *what*
- Renaming variables, functions, or classes for clarity
- Simplifying control flow or reducing nesting
- Aligning code with project conventions after a review
- Cleaning up legacy Java-style patterns in Kotlin code

## Pre-Flight Checklist

- [ ] Read the root `AGENTS.md` — especially the DO/DON'T and Code Style sections
- [ ] Read the module-specific `AGENTS.md` for the module being refactored
- [ ] Understand the *purpose* of the code before changing it — read callers and tests
- [ ] Identify whether changes are purely cosmetic or affect behavior (this skill is for cosmetic/readability only)
- [ ] Check existing tests to ensure refactored code remains covered

## Guiding Principles

### 1. Comments Explain "Why", Never "What"

**Bad** — restates the code:
```kotlin
// Get the bucket metadata
val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
// Check if the key exists
val id = bucketMetadata.getID(key) ?: return null
```

**Good** — explains intent, constraints, or non-obvious decisions:
```kotlin
// S3 returns 404 silently when the key doesn't exist in the bucket,
// rather than throwing an error — we mirror that behavior here.
val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
val id = bucketMetadata.getID(key) ?: return null
```

**Rules:**
- Remove comments that merely restate the code — the code should be self-documenting
- Add comments for *why* a decision was made, *why* an edge case exists, or *why* a workaround is needed
- Reference AWS S3 API behavior when mimicking non-obvious S3 semantics
- Reference GitHub issues or tickets for workarounds: `// Workaround for #1234`
- Use KDoc (`/** */`) for public API documentation; inline comments (`//`) for implementation rationale

### 2. Meaningful Naming Over Comments

If you need a comment to explain *what* code does, rename instead:

**Before:**
```kotlin
// Check if the request has server-side encryption headers
val flag = headers.any { it.key.startsWith("x-amz-server-side-encryption") }
```

**After:**
```kotlin
val hasServerSideEncryption = headers.any { it.key.startsWith("x-amz-server-side-encryption") }
```

**Naming conventions:**
- Booleans: `is-`, `has-`, `should-`, `can-` prefixes (`isVersioned`, `hasLegalHold`)
- Collections: plural nouns (`objects`, `bucketNames`, `partNumbers`)
- Functions: verb phrases describing the action (`verifyBucketExists`, `resolveVersionId`)
- Avoid abbreviations unless universally understood (`id`, `etag`, `kms` are fine; `bktMd`, `objSvc` are not)
- Prefer descriptive names over generic ones (`bucketMetadata` over `data`, `sourceKey` over `key1`)

### 3. Idiomatic Kotlin

Replace Java-isms with Kotlin idioms. Common patterns in this project:

**Scope functions** — use when they improve readability, not just because they exist:
```kotlin
// Good — .let for null-safe transformation chains
val etag = objectStore.getObject(id)?.let { normalizeEtag(it.etag) }

// Good — .also for side effects that shouldn't alter the return value
return objectStore.storeObject(metadata).also {
  log.info("Stored object: {}", it.key)
}

// Bad — scope function obscures simple logic
val name = bucketName.let { it.lowercase() }  // just use: bucketName.lowercase()
```

**Expression bodies** — use for single-expression functions:
```kotlin
// Prefer
fun isVersioned(bucketName: String) =
  bucketStore.getBucketMetadata(bucketName).versioningEnabled

// Over
fun isVersioned(bucketName: String): Boolean {
  return bucketStore.getBucketMetadata(bucketName).versioningEnabled
}
```

**Null safety** — leverage the type system:
```kotlin
// Prefer
val size = metadata?.contentLength ?: 0L

// Over
val size = if (metadata != null) metadata.contentLength else 0L
```

**Named `it` parameter** — always name it in nested or non-trivial lambdas:
```kotlin
// Required by project convention for nested lambdas
objects.filter { obj -> obj.size > 0 }.map { obj -> obj.key }

// Fine for simple, single-level lambdas
keys.map { it.lowercase() }
```

**`when` over `if-else` chains** — use for 3+ branches:
```kotlin
// Prefer
return when {
  versionId != null -> objectStore.getObjectVersion(id, versionId)
  bucket.isVersioned -> objectStore.getLatestVersion(id)
  else -> objectStore.getObject(id)
}
```

**Destructuring** — use when accessing multiple properties of a pair/triple/data class:
```kotlin
val (key, value) = entry
```

### 4. Reduce Nesting and Complexity

**Early returns** — flatten deeply nested code:
```kotlin
// Prefer
fun getObject(bucketName: String, key: String): S3ObjectMetadata? {
  val bucket = bucketStore.getBucketMetadata(bucketName) ?: return null
  val id = bucket.getID(key) ?: return null
  return objectStore.getObject(id)
}

// Over
fun getObject(bucketName: String, key: String): S3ObjectMetadata? {
  val bucket = bucketStore.getBucketMetadata(bucketName)
  if (bucket != null) {
    val id = bucket.getID(key)
    if (id != null) {
      return objectStore.getObject(id)
    }
  }
  return null
}
```

**Extract functions** — break up methods longer than ~30 lines into well-named private functions. The function name serves as documentation.

### 5. KDoc for Public APIs

All public classes and functions should have KDoc that explains:
- **What** the class/function represents in S3 terms (brief)
- **Why** it exists or what S3 behavior it models (important)
- **Gotchas** or non-obvious constraints

```kotlin
/**
 * Handles S3 DeleteObjects (multi-object delete) requests.
 *
 * AWS S3 processes all keys in the request even if some fail — successful
 * and failed deletions are returned together in the response. We replicate
 * this partial-failure behavior rather than failing the entire request.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">AWS DeleteObjects</a>
 */
fun deleteObjects(bucketName: String, delete: Delete): DeleteResult { ... }
```

### 6. Consistent Code Formatting

- **Imports**: No wildcard imports; remove unused imports
- **Blank lines**: One blank line between functions; no multiple consecutive blank lines
- **Parameter lists**: One parameter per line when the declaration exceeds ~100 characters
- **Trailing commas**: Use trailing commas in multi-line parameter lists and collections
- **String templates**: Use `"$variable"` and `"${expression}"` instead of concatenation

## Refactoring Workflow

1. **Read** the code and its tests thoroughly — understand current behavior
2. **Identify** specific readability issues (don't refactor for the sake of it)
3. **Plan** changes that are purely structural — no behavior changes
4. **Apply** changes incrementally, grouped by file
5. **Verify** tests still pass — refactoring must not break anything
6. **Format** with `./mvnw ktlint:format`

## Anti-Patterns to Fix

| Anti-Pattern | Refactor To |
|---|---|
| `if (x != null) { x.foo() }` | `x?.foo()` |
| `if (x == null) throw ...` | `x ?: throw ...` or `requireNotNull(x)` |
| `list.size == 0` | `list.isEmpty()` |
| `list.size > 0` | `list.isNotEmpty()` |
| `"" + value` | `"$value"` or `value.toString()` |
| `Collections.emptyList()` | `emptyList()` |
| `object.equals(other)` | `object == other` |
| `!(x is Foo)` | `x !is Foo` |
| `!(list.contains(x))` | `x !in list` |
| `for (item in list) { result.add(transform(item)) }` | `list.map { transform(it) }` |
| `catch (e: Exception) { /* empty */ }` | At minimum, log the exception |
| Stringly-typed constants | Named constants or enums |
| Magic numbers | Named constants with explanatory names |

## Post-Flight Checklist

- [ ] No behavior changes — only readability and style improvements
- [ ] All existing tests still pass (`./mvnw test -pl server` or `./mvnw verify`)
- [ ] Code formatted with `./mvnw ktlint:format`
- [ ] No new warnings or lint violations
- [ ] Comments explain *why*, not *what*
- [ ] Public APIs have KDoc
- [ ] No unnecessary scope functions or over-engineered abstractions
- [ ] Variable and function names are self-documenting

## Output

Provide clean, idiomatic Kotlin code that is more readable than before, with comments that add genuine value by explaining the reasoning behind decisions.
