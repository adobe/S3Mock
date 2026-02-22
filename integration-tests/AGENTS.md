# Agent Context for S3Mock Integration Tests

Integration tests verifying S3Mock with real AWS SDK clients (v1 and v2).

## Structure

```
integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/
├── S3TestBase.kt           # Base class with s3Client (v2), s3ClientV1
├── BucketIT.kt             # Bucket operations
├── ObjectIT.kt             # Object operations
├── MultipartUploadIT.kt    # Multipart uploads
└── *IT.kt                  # Other tests
```

## Base Class

Extend `S3TestBase` for access to:
- `s3Client` - AWS SDK v2 (default)
- `s3ClientV1` - AWS SDK v1
- `serviceEndpoint`, `serviceEndpointHttp`, `serviceEndpointHttps`

## Test Pattern

Arrange-Act-Assert with unique bucket names (`UUID.randomUUID()`):

```kotlin
class MyFeatureIT : S3TestBase() {
  @Test
  fun `should perform operation`() {
    // Arrange
    val bucketName = givenBucket()

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

**Helper**:
```kotlin
private fun givenBucket() = "test-bucket-${UUID.randomUUID()}".also {
  s3Client.createBucket { it.bucket(it) }
}
```

**Multipart**: Initiate → Upload parts → Complete → Verify

**Errors**:
```kotlin
assertThrows<NoSuchBucketException> {
  s3Client.getObject(...)
}
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
3. Test both SDK v1 (`s3ClientV1`) and v2 (`s3Client`) when applicable
4. Verify: HTTP codes, headers (ETag, Content-Type), XML/JSON bodies, error responses
5. Use actual AWS SDK clients (not mocks)

## Debugging

- Check Docker container logs
- Verify endpoint connectivity
- Compare with AWS S3 API docs
