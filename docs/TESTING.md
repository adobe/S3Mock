# Testing Strategy — S3Mock

## Test Types

| Type | Location | Suffix | Purpose |
|------|----------|--------|---------|
| Unit tests | `server/src/test/kotlin/` | `*Test.kt` | Service, store, and controller logic in isolation |
| Integration tests | `integration-tests/src/test/kotlin/.../its/` | `*IT.kt` | Real AWS SDK v2 against a live Docker container |

## Unit Tests

Use `@SpringBootTest` with `@MockitoBean` for mocking. Extend the appropriate base class:

| Base Class | Use For |
|---|---|
| `ServiceTestBase` | Service-layer tests |
| `StoreTestBase` | Store-layer tests |
| `BaseControllerTest` | Controller slice tests (`@WebMvcTest`) |

Name the class under test **`iut`** (implementation under test); inject with `@Autowired`:

```kotlin
@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(types = [BucketService::class, MultipartService::class, MultipartStore::class])
internal class ObjectServiceTest : ServiceTestBase() {
  @Autowired
  private lateinit var iut: ObjectService

  @Test
  fun `should get object`() {
    whenever(bucketStore.getBucketMetadata("bucket")).thenReturn(bucket)
    whenever(objectStore.getObject(bucket, "key")).thenReturn(s3Object)
    assertThat(iut.getObject("bucket", "key")).isEqualTo(s3Object)
  }
}
```

## Integration Tests

Extend `S3TestBase` for a pre-configured `s3Client` (AWS SDK v2). Accept `TestInfo` as a method parameter and use `givenBucket(testInfo)` for unique bucket names:

```kotlin
internal class MyFeatureIT : S3TestBase() {
  @Test
  fun `should perform operation`(testInfo: TestInfo) {
    // Arrange
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

Access `serviceEndpoint`, `serviceEndpointHttp`, and `serviceEndpointHttps` from `S3TestBase` when needed.

## Conventions

See **[docs/KOTLIN.md](KOTLIN.md)** for Kotlin naming conventions (backtick test names, `internal` visibility, naming patterns).

- **Naming**: Backtick names with descriptive sentences — `` fun `should create bucket successfully`() ``
- **Visibility**: Mark test classes `internal`
- **Pattern**: Arrange-Act-Assert
- **Assertions**: AssertJ (`assertThat(...)`) — use specific matchers, not just `isNotNull()`
- **Error cases**: Use AssertJ, not JUnit `assertThrows`:
  ```kotlin
  assertThatThrownBy { s3Client.deleteBucket { it.bucket(bucketName) } }
    .isInstanceOf(AwsServiceException::class.java)
    .hasMessageContaining("Status Code: 409")
  ```
- **Independence**: Each test creates its own resources — no shared state, UUID-based bucket names
- **Legacy names**: Refactor `testSomething` camelCase names to backtick style when touching existing tests

## Running Tests

```bash
./mvnw test -pl server                                          # Unit tests only
./mvnw verify -pl integration-tests                             # All integration tests
./mvnw verify -pl integration-tests -Dit.test=BucketIT          # Specific class
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket  # Specific method
./mvnw test -pl server -DskipDocker                             # Skip Docker
```

> Integration tests require Docker to be running.

## Troubleshooting

- **Docker not running**: Integration tests will fail — start Docker first
- **Port conflict**: Check `lsof -i :9090`
- **Flaky test**: Look for shared state or ordering dependencies
- **Compilation error**: Run `./mvnw clean install -DskipDocker -DskipTests` first

## Checklist

- [ ] Tests pass locally
- [ ] Both success and failure cases covered
- [ ] Tests are independent (no shared state, UUID bucket names)
- [ ] Assertions are specific
- [ ] Run `./mvnw ktlint:format`
