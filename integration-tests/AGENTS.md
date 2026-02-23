# Agent Context for S3Mock Integration Tests

> Inherits all conventions from the [root AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Integration tests verifying S3Mock with real AWS SDK v2 clients.

## Structure

```
integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/
├── S3TestBase.kt           # Base class with s3Client (v2)
├── *IT.kt                  # Test classes (BucketIT, ObjectIT, MultipartUploadIT, etc.)
```

## Base Class

Extend `S3TestBase` for access to:
- `s3Client` — AWS SDK v2
- `serviceEndpoint`, `serviceEndpointHttp`, `serviceEndpointHttps`

## Module-Specific Rules

- Extend **`S3TestBase`** for all integration tests
- Accept **`testInfo: TestInfo`** parameter in test methods for unique resource naming
- Use **`givenBucket(testInfo)`** for bucket creation — don't create your own helper
- Use **Arrange-Act-Assert** pattern consistently
- Verify HTTP codes, headers (ETag, Content-Type), XML/JSON bodies, and error responses
- DON'T mock AWS SDK clients — use actual SDK clients against S3Mock

## Test Pattern

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

**Multipart**: Initiate → Upload parts → Complete → Verify

**Errors** (use AssertJ, not JUnit `assertThrows`):
```kotlin
assertThatThrownBy { s3Client.deleteBucket { it.bucket(bucketName) } }
  .isInstanceOf(AwsServiceException::class.java)
  .hasMessageContaining("Status Code: 409")
```

## Running

```bash
./mvnw verify -pl integration-tests
./mvnw verify -pl integration-tests -Dit.test=BucketIT
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket
```
