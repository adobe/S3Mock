# Agent Context for S3Mock Integration Tests

This module contains integration tests that verify S3Mock against real AWS SDK clients.

## Module Structure

```
integration-tests/
├── src/test/kotlin/
│   └── com/adobe/testing/s3mock/its/
│       ├── S3TestBase.kt           # Base class for all integration tests
│       ├── BucketIT.kt             # Bucket operation tests
│       ├── ObjectIT.kt             # Object operation tests
│       ├── MultipartUploadIT.kt   # Multipart upload tests
│       └── *IT.kt                  # Other integration tests
└── pom.xml
```

## Test Base Class

All integration tests extend `S3TestBase`:

```kotlin
abstract class S3TestBase {
  companion object {
    lateinit var s3Client: S3Client              // AWS SDK v2
    lateinit var s3ClientV1: AmazonS3            // AWS SDK v1
    lateinit var serviceEndpoint: String         // S3Mock endpoint
    lateinit var serviceEndpointHttp: String     // HTTP endpoint
    lateinit var serviceEndpointHttps: String    // HTTPS endpoint
  }
}
```

### Provided Clients

- `s3Client` - AWS SDK v2 client (default)
- `s3ClientV1` - AWS SDK v1 client (for v1 compatibility tests)
- `httpClient` - Configured HTTP client for direct HTTP requests

## Test Structure

Follow the Arrange-Act-Assert pattern:

```kotlin
class MyFeatureIT : S3TestBase() {

  @Test
  fun `should perform operation successfully`() {
    // Arrange - Setup test data
    val bucketName = "test-bucket-${UUID.randomUUID()}"
    val key = "test-key"
    s3Client.createBucket { it.bucket(bucketName) }

    // Act - Perform the operation
    s3Client.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
      RequestBody.fromString("test content")
    )

    // Assert - Verify results
    val response = s3Client.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    )

    assertThat(response.response().contentLength()).isEqualTo(12L)
    assertThat(response.readAllBytes().decodeToString()).isEqualTo("test content")
  }
}
```

## Test Naming Conventions

- Class names: End with `IT` (e.g., `BucketIT.kt`)
- Test methods: Use descriptive names with backticks
  - `` `should perform action when condition` ``
  - `` `should throw exception when invalid input` ``

## Common Test Patterns

### Testing Object Operations

```kotlin
@Test
fun `should store and retrieve object`() {
  val bucketName = givenBucket()
  val key = "my-key"
  val content = "test content"

  s3Client.putObject(
    PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build(),
    RequestBody.fromString(content)
  )

  val response = s3Client.getObject(
    GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()
  )

  assertThat(response.readAllBytes().decodeToString()).isEqualTo(content)
}
```

### Testing Bucket Operations

```kotlin
@Test
fun `should create and list buckets`() {
  val bucketName = "test-bucket-${UUID.randomUUID()}"

  s3Client.createBucket { it.bucket(bucketName) }

  val buckets = s3Client.listBuckets()

  assertThat(buckets.buckets())
    .anyMatch { it.name() == bucketName }
}
```

### Testing Multipart Uploads

```kotlin
@Test
fun `should complete multipart upload`() {
  val bucketName = givenBucket()
  val key = "large-file"

  // Initiate
  val uploadId = s3Client.createMultipartUpload(
    CreateMultipartUploadRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()
  ).uploadId()

  // Upload parts
  val part1 = s3Client.uploadPart(
    UploadPartRequest.builder()
      .bucket(bucketName)
      .key(key)
      .uploadId(uploadId)
      .partNumber(1)
      .build(),
    RequestBody.fromString("part1")
  )

  val part2 = s3Client.uploadPart(
    UploadPartRequest.builder()
      .bucket(bucketName)
      .key(key)
      .uploadId(uploadId)
      .partNumber(2)
      .build(),
    RequestBody.fromString("part2")
  )

  // Complete
  s3Client.completeMultipartUpload(
    CompleteMultipartUploadRequest.builder()
      .bucket(bucketName)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(
        CompletedMultipartUpload.builder()
          .parts(
            CompletedPart.builder().partNumber(1).eTag(part1.eTag()).build(),
            CompletedPart.builder().partNumber(2).eTag(part2.eTag()).build()
          )
          .build()
      )
      .build()
  )

  // Verify
  val response = s3Client.getObject(
    GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()
  )

  assertThat(response.readAllBytes().decodeToString()).isEqualTo("part1part2")
}
```

### Testing Error Conditions

```kotlin
@Test
fun `should throw NoSuchBucket exception`() {
  assertThrows<NoSuchBucketException> {
    s3Client.getObject(
      GetObjectRequest.builder()
        .bucket("non-existent-bucket")
        .key("key")
        .build()
    )
  }
}
```

### Testing with Headers

```kotlin
@Test
fun `should handle custom metadata`() {
  val bucketName = givenBucket()
  val key = "key-with-metadata"

  s3Client.putObject(
    PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .metadata(mapOf("custom-key" to "custom-value"))
      .build(),
    RequestBody.fromString("content")
  )

  val response = s3Client.headObject(
    HeadObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()
  )

  assertThat(response.metadata()).containsEntry("custom-key", "custom-value")
}
```

## Helper Methods

### Bucket Creation

```kotlin
private fun givenBucket(): String {
  val bucketName = "test-bucket-${UUID.randomUUID()}"
  s3Client.createBucket { it.bucket(bucketName) }
  return bucketName
}
```

### Random Test Data

```kotlin
private fun randomBytes(size: Int): ByteArray {
  val bytes = ByteArray(size)
  Random.nextBytes(bytes)
  return bytes
}
```

## Assertions

Use AssertJ for fluent assertions:

```kotlin
// Basic assertions
assertThat(result).isNotNull()
assertThat(result).isEqualTo(expected)
assertThat(list).hasSize(3)
assertThat(list).contains(item)

// Collection assertions
assertThat(buckets.buckets())
  .hasSize(1)
  .anyMatch { it.name() == bucketName }

// Exception assertions
assertThrows<NoSuchBucketException> {
  s3Client.deleteBucket { it.bucket("non-existent") }
}
```

## XML Response Verification

For testing XML responses directly:

```kotlin
import org.xmlunit.assertj3.XmlAssert

@Test
fun `should return valid XML`() {
  val xml = """
    <ListBucketResult>
      <Name>test-bucket</Name>
    </ListBucketResult>
  """.trimIndent()

  XmlAssert.assertThat(xml)
    .nodesByXPath("//Name")
    .exist()
}
```

## Test Execution

```bash
# Run all integration tests
./mvnw verify -pl integration-tests

# Run specific test class
./mvnw verify -pl integration-tests -Dit.test=BucketIT

# Run specific test method
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket

# Skip Docker build
./mvnw verify -pl integration-tests -DskipDocker
```

## S3Mock Test Configuration

S3Mock is started automatically via Docker Maven Plugin. Configuration in `pom.xml`:

```xml
<plugin>
  <groupId>io.fabric8</groupId>
  <artifactId>docker-maven-plugin</artifactId>
  <configuration>
    <images>
      <image>
        <name>adobe/s3mock:latest</name>
        <run>
          <ports>
            <port>it.s3mock.port_http:9090</port>
            <port>it.s3mock.port_https:9191</port>
          </ports>
        </run>
      </image>
    </images>
  </configuration>
</plugin>
```

## Test Best Practices

1. **Isolation**: Each test is independent, no shared state
2. **Cleanup**: Tests clean up automatically (or rely on fresh S3Mock instance)
3. **Naming**: Use unique bucket names with UUID to avoid conflicts
4. **Speed**: Keep tests fast, avoid unnecessary delays
5. **Coverage**: Test both AWS SDK v1 and v2 when applicable
6. **Real SDK**: Use actual AWS SDK clients, not mocks

## Testing Both SDK Versions

When testing features with both SDK versions:

```kotlin
@Test
fun `should work with SDK v1`() {
  val bucketName = givenBucket()

  // Use s3ClientV1 (AWS SDK v1)
  s3ClientV1.putObject(bucketName, "key", "content")

  val content = s3ClientV1.getObjectAsString(bucketName, "key")
  assertThat(content).isEqualTo("content")
}

@Test
fun `should work with SDK v2`() {
  val bucketName = givenBucket()

  // Use s3Client (AWS SDK v2)
  s3Client.putObject(
    PutObjectRequest.builder().bucket(bucketName).key("key").build(),
    RequestBody.fromString("content")
  )

  val response = s3Client.getObject(
    GetObjectRequest.builder().bucket(bucketName).key("key").build()
  )

  assertThat(response.readAllBytes().decodeToString()).isEqualTo("content")
}
```

## Verifying S3Mock Behavior

Integration tests verify:
- Correct HTTP status codes
- Proper response headers (ETag, Content-Type, etc.)
- Valid XML/JSON response bodies
- Error responses match AWS S3
- Edge cases and boundary conditions

## Debugging Failed Tests

1. Check S3Mock logs (Docker container logs)
2. Verify endpoint connectivity
3. Check request/response with HTTP client debugging
4. Validate test data and assumptions
5. Compare with AWS S3 API documentation

## References

- Main project context: `/AGENTS.md`
- Server implementation: `/server/AGENTS.md`
- AWS SDK v2: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/
- AWS SDK v1: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/
