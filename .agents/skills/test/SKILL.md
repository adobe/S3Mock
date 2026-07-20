---
name: test
description: Write, update, and fix tests in S3Mock. Use when asked to add test coverage, create test cases, fix failing tests, or verify test correctness. Covers unit tests (*Test.kt in server/), controller slice tests (@WebMvcTest), and integration tests (*IT.kt in integration-tests/ against a live Docker container).
---

# Test Skill

## Entry Criteria

Use to add/update unit tests (`*Test.kt`) or integration tests (`*IT.kt`), fix failing tests, or verify test coverage.

## Before Starting

1. Read root `AGENTS.md`, the relevant module `AGENTS.md`, and **[docs/TESTING.md](../../../docs/TESTING.md)** for base classes, conventions, and running commands.
2. If existing tests have structural problems (poor naming, shared state, weak assertions), invoke the **`refactor` skill** first.

## Execution Steps

1. Select the base class per `docs/TESTING.md` § Unit Tests, or `S3TestBase` per **[integration-tests/AGENTS.md](../../../integration-tests/AGENTS.md)** for integration tests.
2. Write tests following `docs/TESTING.md` conventions (naming, structure, assertions, independence).
3. Cover both success and failure/error paths.
4. Update the copyright year in every modified file (`INVARIANTS.md`).
5. Invoke the **`lint` skill**.
6. Run `make test` (unit) or `make integration-tests` (integration) to verify locally.

## Completion Criteria

- [ ] All new/changed code paths covered, including failure cases
- [ ] Correct base class and conventions used (per `docs/TESTING.md`)
- [ ] Tests pass locally
- [ ] Copyright year updated in every modified file
- [ ] Lint gates pass (via `lint` skill)

## Resources

- [`AGENTS.md`](../../../AGENTS.md), [`INVARIANTS.md`](../../../INVARIANTS.md)
- Relevant module `AGENTS.md` (`server/AGENTS.md` or `integration-tests/AGENTS.md`)
- [`docs/TESTING.md`](../../../docs/TESTING.md), [`docs/KOTLIN.md`](../../../docs/KOTLIN.md)
