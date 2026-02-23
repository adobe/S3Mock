---
name: test
description: Write, update, or fix tests. Use when asked to test code, create test cases, or debug failing tests.
---

# Test Skill — S3Mock

Create and maintain tests for S3Mock (JUnit 5, Mockito, AssertJ, AWS SDK v2).

## When to Use

- Writing new unit or integration tests
- Fixing failing tests
- Adding test coverage for S3 operations
- Creating test fixtures or helpers

## Pre-Flight Checklist

- [ ] Read `AGENTS.md` — especially DO/DON'T and Testing sections
- [ ] Identify test type needed (unit vs. integration — see below)
- [ ] Review existing test patterns in the target module
- [ ] For integration tests, review `S3TestBase` for available helpers

## Test Types in S3Mock

### Unit Tests (`*Test.kt`)
- **Location**: `server/src/test/kotlin/com/adobe/testing/s3mock/`
- **Framework**: JUnit 5 + `@SpringBootTest` with `@MockitoBean` for mocking
- **Assertions**: AssertJ (`assertThat(...)`)
- **Purpose**: Test services and stores in isolation with Spring context and mocked dependencies
- **Pattern**:
```kotlin
@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(types = [BucketService::class, MultipartService::class, MultipartStore::class])
internal class ObjectServiceTest : ServiceTestBase() {
  @Autowired
  private lateinit var iut: ObjectService

  @Test
  fun `should delete object`() {
    val bucketName = "bucket"
    val key = "key"
    givenBucketWithContents(bucketName, "", listOf(givenS3Object(key)))

    iut.deleteObject(bucketName, key)

    assertThat(iut.getObject(bucketName, key)).isNull()
  }
}
```

### Integration Tests (`*IT.kt`)
- **Location**: `integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/`
- **Base class**: Extend `S3TestBase` for access to pre-configured `s3Client` (AWS SDK v2)
- **Assertions**: AssertJ
- **Purpose**: Test S3Mock end-to-end with real AWS SDK clients against the Docker container
- **Pattern**:
```kotlin
internal class MyFeatureIT : S3TestBase() {
  @Test
  fun `should perform operation`(testInfo: TestInfo) {
    // Arrange — always use unique bucket names
    val bucketName = givenBucket(testInfo)

    // Act
    s3Client.putObject(
      PutObjectRequest.builder().bucket(bucketName).key("key").build(),
      RequestBody.fromString("content")
    )

    // Assert
    val response = s3Client.getObject(
      GetObjectRequest.builder().bucket(bucketName).key("key").build()
    )
    assertThat(response.readAllBytes().decodeToString()).isEqualTo("content")
  }
}
```

## Test Standards

- **Naming**: Use backtick names with descriptive sentences: `` fun `should create bucket successfully`() ``
  - Legacy `testSomething` camelCase naming exists in older tests — refactor to backtick style when touching those tests
- **Independence**: Each test creates its own resources — never share state between tests
- **Bucket names**: Use `givenBucket(testInfo)` from `S3TestBase` in integration tests for unique names
- **Arrange-Act-Assert**: Follow this pattern consistently
- **Both paths**: Test success cases AND error/exception cases
- **Error assertions**: Use `assertThatThrownBy { ... }.isInstanceOf(AwsServiceException::class.java)` (AssertJ), not `assertThrows`
- **SDK version**: Use AWS SDK v2 (`s3Client`) only — SDK v1 has been removed
- **No JUnit 4**: Use JUnit 5 exclusively (`@Test` from `org.junit.jupiter.api`)
- **Visibility**: Mark test classes as `internal`
- **No MockitoExtension**: Use `@SpringBootTest` with `@MockitoBean` for mocking — never `@ExtendWith(MockitoExtension::class)`
- **Mocking**: Use `@MockitoBean` (class-level `types` or field-level) instead of `@Mock` / `@InjectMocks`
- **Injection**: Use `@Autowired` for the class under test in Spring Boot tests

## Running Tests

```bash
# Unit tests (server module)
./mvnw test -pl server

# All integration tests
./mvnw verify -pl integration-tests

# Specific integration test class
./mvnw verify -pl integration-tests -Dit.test=BucketIT

# Specific test method
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket

# Skip Docker (for unit tests only)
./mvnw test -pl server -DskipDocker
```

## Post-Flight Checklist

- [ ] Tests pass locally (`./mvnw test` or `./mvnw verify`)
- [ ] Tests are independent (can run in any order)
- [ ] Both success and failure cases covered
- [ ] Assertions are specific (not just `isNotNull()`)
- [ ] No hardcoded bucket names (use UUID)
- [ ] Code style passes (`./mvnw ktlint:format`)

## Troubleshooting Failing Tests

- **Docker not running**: Integration tests require Docker — start Docker Desktop
- **Port conflict**: Ports 9090/9191 may be in use — check with `lsof -i :9090`
- **Flaky test**: Ensure test independence — check for shared state or ordering dependencies
- **Compilation error**: Run `./mvnw clean install -DskipDocker -DskipTests` first

## Output

Provide complete, runnable Kotlin tests following S3Mock conventions and the Arrange-Act-Assert pattern.
