---
name: review
description: Review code changes in pull requests or local diffs for S3Mock. Use when asked to review a PR, inspect changes, or provide structured feedback on correctness, conventions, test quality, and documentation. Outputs findings structured as Must fix / Should fix / Nit.
---

# Code Review Skill

## Entry Criteria

Use this skill when asked to:
- Review a pull request
- Inspect a local diff or branch
- Provide structured feedback on code quality

## Before Starting

Read `AGENTS.md` (root) and the relevant module `AGENTS.md` — especially the DO/DON'T, Architecture, and Code Style sections.

## Review Scope

Evaluate in this priority order:

### 1. Correctness

- Does the code do what it claims? Check edge cases and error paths.
- Do XML element/attribute names match the [AWS S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) exactly? See `INVARIANTS.md`.
- Are `S3Exception` constants used — not custom exception classes? See `INVARIANTS.md`.
- Does layering hold — business logic only in services, HTTP mapping only in controllers, persistence only in stores? See `INVARIANTS.md`.

### 2. Convention Violations

Flag as **Must fix**:

- Copyright header not updated to `2017-<current year>` in every modified file
- AWS SDK v1 usage
- JUnit 4 usage
- Dependency versions declared in sub-module POMs
- Business logic in controllers; raw string responses from controllers
- Controllers catching exceptions
- Legacy Jackson XML annotations from `com.fasterxml.jackson.dataformat.xml.annotation` — must use `tools.jackson` packages in 5.x
- `@Autowired` or field injection in production code (see `docs/SPRING.md`)
- `@Mock`/`@InjectMocks`/`@ExtendWith(MockitoExtension::class)` in tests — use `@SpringBootTest` + `@MockitoBean` (see `docs/SPRING.md`)
- `var` on public API properties (see `docs/KOTLIN.md`)
- Apache Commons usage where Kotlin stdlib suffices

### 3. Test Quality

- Are new/changed code paths covered by tests?
- Correct base class used (`ServiceTestBase`, `StoreTestBase`, `BaseControllerTest`, or `S3TestBase`)?
- No shared state; `testInfo`-based or UUID-based resource names?
- Backtick names, `internal class`, AssertJ assertions?
- Integration tests use `givenBucket(testInfo)`?

### 4. Kotlin Idioms

See [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) for the full list of idioms and anti-patterns to check (null safety, expression bodies, scope functions, `isEmpty()`/`isNotEmpty()`, `when` expressions, naming).

### 5. Documentation

- Is `CHANGELOG.md` updated for user-facing changes?
- Is the `README.md` operations table updated for new S3 operations?
- Do new public APIs have KDoc?

## Output Format

Structure feedback as:

- **Must fix** — blocks merge: correctness issues, `INVARIANTS.md` constraint violations, missing tests
- **Should fix** — strongly recommended: idiom improvements, missing docs. Suggest invoking the **`refactor` skill** for purely style improvements rather than asking the author to add more code.
- **Nit** — optional style suggestions

For each finding, reference the specific `AGENTS.md` rule, `INVARIANTS.md` constraint, or AWS API doc.

## Execution Steps

1. Read root + relevant module `AGENTS.md`.
2. Evaluate all five categories above in priority order.
3. If style issues found, suggest the author invoke the **`lint` skill**.
4. If readability-only issues found, suggest the author invoke the **`refactor` skill**.
5. Confirm `CHANGELOG.md` is updated if the change is user-facing.
6. Produce structured output using the Must fix / Should fix / Nit format with file/line references.

## Completion Criteria

- [ ] All five categories evaluated
- [ ] Must fix / Should fix / Nit structure used
- [ ] Each finding references its source rule or constraint
- [ ] CI gate readiness assessed (ktlint, Checkstyle, tests, Docker build)
- [ ] `CHANGELOG.md` check confirmed

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — architecture, DO/DON'T, CI gates
- [`INVARIANTS.md`](../../../INVARIANTS.md) — hard constraints; source for Must fix classifications
- Relevant module `AGENTS.md`
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) — Kotlin idioms and anti-patterns
- [`docs/SPRING.md`](../../../docs/SPRING.md) — Spring Boot patterns, DI, exception handling, testing
- [`docs/TESTING.md`](../../../docs/TESTING.md) — base classes, test conventions
