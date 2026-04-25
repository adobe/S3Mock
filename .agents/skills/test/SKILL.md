---
name: test
description: Write, update, and fix tests in S3Mock. Use when asked to add test coverage, create test cases, fix failing tests, or verify test correctness. Covers unit tests (*Test.kt in server/), controller slice tests (@WebMvcTest), and integration tests (*IT.kt in integration-tests/ against a live Docker container).
---

# Test Skill

## Entry Criteria

Use this skill when asked to:
- Add or update unit tests (`*Test.kt`) in `server/`
- Add or update integration tests (`*IT.kt`) in `integration-tests/`
- Fix failing tests
- Verify test correctness or coverage

## Before Starting

1. Read `AGENTS.md` (root) and the relevant module `AGENTS.md`.
2. Read `docs/TESTING.md` for the full testing strategy, base classes, and patterns.
3. If existing tests have structural problems (poor naming, shared state, weak assertions), invoke the **`refactor` skill** to fix them before adding new tests.

## Base Class Selection

Always extend the correct base class — never write tests without one:

| Test type | Base class | Module |
|---|---|---|
| Service unit tests | `ServiceTestBase` | `server/` |
| Store unit tests | `StoreTestBase` | `server/` |
| Controller slice tests (`@WebMvcTest`) | `BaseControllerTest` | `server/` |
| Integration tests (live Docker container) | `S3TestBase` | `integration-tests/` |

## Conventions

- **Naming**: backtick function names — `` fun `should create bucket when name is valid`() ``
- **Structure**: Arrange → Act → Assert
- **Independence**: each test creates its own resources; no shared state between tests
- **Assertions**: AssertJ — `assertThat(result).isEqualTo(expected)`, not bare `isNotNull()`
- **Error cases**: `assertThatThrownBy { ... }.isInstanceOf(AwsServiceException::class.java)`
- **Visibility**: `internal class`
- **Unit under test**: name it `iut`, injected with `@Autowired`

## Integration Test Conventions

- Accept `testInfo: TestInfo` as a method parameter for unique resource naming
- Use `givenBucket(testInfo)` for bucket creation — do not write your own helper
- Use actual AWS SDK v2 clients against S3Mock — do not mock SDK clients (see `INVARIANTS.md`)

## Execution Steps

1. Select the correct base class for the test type.
2. Write tests following the naming and structure conventions above.
3. Cover both success paths and failure/error paths.
4. Keep tests independent — use `testInfo`-based or UUID-based resource names.
5. Update the copyright year in every file you modify — see `INVARIANTS.md`.
6. Invoke the **`lint` skill** to fix formatting and verify style gates pass.
7. Verify all tests pass locally: `make test` (unit) or `make integration-tests` (integration).

## Completion Criteria

- [ ] All new/changed code paths covered by tests
- [ ] Both success and failure cases tested
- [ ] Tests pass locally
- [ ] Correct base class used
- [ ] Backtick naming, `internal class`, AssertJ assertions used
- [ ] No shared state between tests
- [ ] Copyright year updated in every modified file
- [ ] Lint gates pass (via `lint` skill)

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — DO/DON'T, build commands
- [`INVARIANTS.md`](../../../INVARIANTS.md) — hard constraints (copyright, SDK version, test framework restrictions)
- Relevant module `AGENTS.md` (`server/AGENTS.md` or `integration-tests/AGENTS.md`)
- [`docs/TESTING.md`](../../../docs/TESTING.md) — full testing strategy, base classes, patterns, running commands
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) — naming conventions, `internal class`, backtick names
