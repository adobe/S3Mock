---
name: test
description: Write, update, or fix tests. Use when asked to test code, create test cases, or debug failing tests.
---

# Test Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before writing tests — especially the Testing, DO/DON'T sections and base classes.

## Test Types

### Unit Tests (`*Test.kt`) — `server/src/test/kotlin/.../`
- `@SpringBootTest` with `@MockitoBean` for mocking (see AGENTS.md for details)
- Extend the appropriate base class: `ServiceTestBase`, `StoreTestBase`, or `BaseControllerTest`
- Name the class under test `iut`
- Use `@Autowired` for the class under test

### Integration Tests (`*IT.kt`) — `integration-tests/src/test/kotlin/.../its/`
- Extend `S3TestBase` for pre-configured `s3Client` (AWS SDK v2)
- Use `givenBucket(testInfo)` for unique bucket names
- Tests run against Docker container — Docker must be running

## Key Conventions (see AGENTS.md for full list)

- **Naming**: Backtick names: `` fun `should create bucket successfully`() ``
- **Pattern**: Arrange-Act-Assert
- **Independence**: Each test creates its own resources — no shared state
- **Assertions**: AssertJ (`assertThat(...)`) — specific assertions, not just `isNotNull()`
- **Error cases**: `assertThatThrownBy { ... }.isInstanceOf(AwsServiceException::class.java)`
- **Visibility**: `internal class`

## Running Tests

```bash
./mvnw test -pl server                                    # Unit tests
./mvnw verify -pl integration-tests                       # All integration tests
./mvnw verify -pl integration-tests -Dit.test=BucketIT    # Specific class
./mvnw test -pl server -DskipDocker                       # Skip Docker
```

## Troubleshooting

- **Docker not running**: Integration tests require Docker
- **Port conflict**: Check `lsof -i :9090`
- **Flaky test**: Check for shared state or ordering dependencies
- **Compilation error**: Run `./mvnw clean install -DskipDocker -DskipTests` first

## Checklist

- [ ] Tests pass locally
- [ ] Both success and failure cases covered
- [ ] Tests are independent (no shared state, UUID bucket names)
- [ ] Assertions are specific
- [ ] Run `./mvnw ktlint:format`
