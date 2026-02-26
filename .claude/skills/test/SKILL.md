---
name: test
description: Write, update, or fix tests. Use when asked to test code, create test cases, or debug failing tests.
---

# Test Skill — S3Mock

Read **[docs/TESTING.md](../../../docs/TESTING.md)**, **[docs/KOTLIN.md](../../../docs/KOTLIN.md)**, and `AGENTS.md` (root + relevant module) before writing tests — they define test types, base classes, naming conventions, and running commands.

## Key Conventions (from AGENTS.md)

- **Naming**: Backtick names: `` fun `should create bucket successfully`() ``
- **Pattern**: Arrange-Act-Assert
- **Independence**: Each test creates its own resources — no shared state
- **Assertions**: AssertJ (`assertThat(...)`) — specific assertions, not just `isNotNull()`
- **Error cases**: `assertThatThrownBy { ... }.isInstanceOf(AwsServiceException::class.java)`
- **Visibility**: `internal class`

## Checklist

- [ ] Read `docs/TESTING.md` and root + module `AGENTS.md`
- [ ] If existing tests have structural problems (poor naming, shared state, weak assertions), invoke the **`refactor` skill** to fix them rather than working around them
- [ ] Verify tests pass locally
- [ ] Cover both success and failure cases
- [ ] Keep tests independent (no shared state, UUID bucket names)
- [ ] Use specific assertions
- [ ] Run `make format`
