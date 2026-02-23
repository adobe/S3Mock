# Agent Context for S3Mock Integration Tests

Integration tests verifying S3Mock with real AWS SDK v2 clients.

## Structure

```
integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/
├── S3TestBase.kt           # Base class with s3Client (v2)
├── BucketIT.kt             # Bucket operations
├── ObjectIT.kt             # Object operations
├── MultipartUploadIT.kt    # Multipart uploads
└── *IT.kt                  # Other tests
```

## Base Class

Extend `S3TestBase` for access to:
- `s3Client` - AWS SDK v2
- `serviceEndpoint`, `serviceEndpointHttp`, `serviceEndpointHttps`

## DO / DON'T

### DO
- Extend `S3TestBase` for all integration tests
- Use **backtick test names**: `` fun `should create bucket and upload object`(testInfo: TestInfo) ``
- Use **unique bucket names** via `givenBucket(testInfo)` or `UUID.randomUUID()`
- Use **Arrange-Act-Assert** pattern consistently
- Accept `testInfo: TestInfo` parameter in test methods for unique resource naming
- Verify HTTP codes, headers (ETag, Content-Type), XML/JSON bodies, and error responses
- Refactor legacy `testSomething` camelCase names to backtick style when touching tests

### DON'T
- DON'T use AWS SDK v1 — it has been removed in 5.x
- DON'T share state between tests — each test must be self-contained
- DON'T hardcode bucket names — use `UUID.randomUUID()` for uniqueness
- DON'T mock AWS SDK clients — use actual SDK clients against S3Mock

## Test Pattern

Arrange-Act-Assert with `testInfo` for unique bucket names:

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

## Common Patterns

**Bucket creation**: Use `givenBucket(testInfo)` from `S3TestBase` — don't create your own helper.

**Multipart**: Initiate → Upload parts → Complete → Verify

**Errors** (use AssertJ, not JUnit `assertThrows`):
```kotlin
assertThatThrownBy { s3Client.deleteBucket { it.bucket(bucketName) } }
  .isInstanceOf(AwsServiceException::class.java)
  .hasMessageContaining("Status Code: 409")
```

**Metadata**:
```kotlin
s3Client.putObject(
  PutObjectRequest.builder()
    .metadata(mapOf("key" to "value"))
    .build(),
  RequestBody.fromString("content")
)
```

## Assertions

AssertJ:
```kotlin
assertThat(result).isNotNull()
assertThat(list).hasSize(3).contains(item)
assertThat(buckets.buckets()).anyMatch { it.name() == bucketName }
```

## Running

```bash
./mvnw verify -pl integration-tests
./mvnw verify -pl integration-tests -Dit.test=BucketIT
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket
./mvnw verify -pl integration-tests -DskipDocker
```

## Best Practices

1. Independent tests (no shared state)
2. Unique bucket names with UUID
3. Use AWS SDK v2 (`s3Client`) — SDK v1 has been removed
4. Verify: HTTP codes, headers (ETag, Content-Type), XML/JSON bodies, error responses
5. Use actual AWS SDK clients (not mocks)

## Debugging

- Check Docker container logs
- Verify endpoint connectivity
- Compare with AWS S3 API docs
