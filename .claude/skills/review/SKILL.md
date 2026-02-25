---
name: review
description: Review code changes in PRs or local diffs. Use when asked to review a PR, inspect changes, or provide feedback on code quality.
---

# Code Review Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before reviewing — especially DO/DON'T, Code Style, and Architecture sections.

## Review Scope

Evaluate changes against these categories, in priority order:

### 1. Correctness
- Does the code do what it claims? Check edge cases and error paths.
- Do XML element/attribute names match the [AWS S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) exactly?
- Are `S3Exception` constants used correctly (not new exception classes)?
- Does the layering hold? Business logic in services, HTTP mapping in controllers, persistence in stores.

### 2. Convention Violations (from AGENTS.md DO/DON'T)
- `@Autowired` or field injection in production code
- `@Mock` / `@InjectMocks` / `@ExtendWith(MockitoExtension::class)` instead of `@SpringBootTest` + `@MockitoBean`
- `var` on public API properties
- AWS SDK v1 usage
- Apache Commons dependencies where Kotlin stdlib suffices
- Dependency versions declared in sub-module POMs
- Business logic in controllers or raw string responses
- Legacy `testSomething` naming in new or touched tests

### 3. Test Quality
- Are new/changed code paths covered by tests?
- Unit tests (`*Test.kt`): extend correct base class (`ServiceTestBase`, `StoreTestBase`, `BaseControllerTest`)?
- Integration tests (`*IT.kt`): extend `S3TestBase`, use `givenBucket(testInfo)`?
- Test independence: no shared state, UUID bucket names?
- Backtick names, `internal class`, AssertJ assertions?

### 4. Kotlin Idioms

See **[docs/KOTLIN.md](../../../docs/KOTLIN.md)** for the full list of idioms and anti-patterns to check (null safety, expression bodies, named `it`, `when`, `isEmpty()`/`isNotEmpty()`, string templates).

### 5. Documentation & Changelog
- Is `CHANGELOG.md` updated for user-facing changes?
- Is `README.md` operations table updated for new S3 operations?
- Do public APIs have KDoc?

## Review Output Format

Structure feedback as:

- **Must fix** — blocks merge (correctness issues, convention violations, missing tests)
- **Should fix** — strongly recommended (idiom improvements, missing docs)
- **Nit** — optional style suggestions

For each finding, reference the specific AGENTS.md rule or AWS API doc where applicable.

## Checklist

- [ ] Read root + relevant module `AGENTS.md`
- [ ] Check all categories above in priority order
- [ ] Verify CI gates will pass (ktlint, checkstyle, tests, Docker build)
- [ ] Confirm `CHANGELOG.md` is updated if needed
- [ ] Provide actionable feedback with specific file/line references
