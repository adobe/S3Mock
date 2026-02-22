# Agent Context for S3Mock

Lightweight S3 API mock server for local integration testing.

## Tech Stack
- **Kotlin 2.3** (JVM 17), Spring Boot 4.0.x, Maven 3.9+
- **Testing**: JUnit 5, Mockito, AssertJ, Testcontainers
- **Container**: Docker/Alpine

## Structure
```
server/              # Core implementation (Controller→Service→Store)
integration-tests/   # AWS SDK integration tests
testsupport/         # JUnit 4/5, Testcontainers, TestNG integrations
```

## Architecture

**Layered**: Controller (REST) → Service (logic) → Store (filesystem)

**Key packages**: `controller/`, `service/`, `store/`, `dto/`

## Code Style

**Kotlin idioms**: Data classes for DTOs, null safety, expression bodies, constructor injection

**Spring**: `@RestController`, `@Service`, `@Component`, constructor injection over field injection

**Example**:
```kotlin
@RestController
class ObjectController(private val objectService: ObjectService) {
  @GetMapping("/{bucketName:.+}/{*key}")
  fun getObject(@PathVariable bucketName: String, @PathVariable key: String) =
    objectService.getObject(bucketName, key).let {
      ResponseEntity.ok().header("ETag", it.etag).body(it.data)
    }
}
```

## XML Serialization

Jackson XML with AWS-compatible structure. Key annotations:
- `@JacksonXmlRootElement(localName = "...")`
- `@JacksonXmlProperty(localName = "...")`
- `@JacksonXmlElementWrapper(useWrapping = false)` for collections

## Storage

Filesystem layout:
```
<root>/<bucket>/bucketMetadata.json
<root>/<bucket>/<uuid>/binaryData + objectMetadata.json
<root>/<bucket>/<uuid>/<version-id>-binaryData  # versioning
<root>/<bucket>/multiparts/<upload-id>/<part>.part
```

## Configuration

Environment variables (prefix: `COM_ADOBE_TESTING_S3MOCK_STORE_`):
- `ROOT` - storage directory
- `RETAIN_FILES_ON_EXIT` - keep files after shutdown
- `REGION` - AWS region (default: us-east-1)
- `INITIAL_BUCKETS` - comma-separated bucket names
- `VALID_KMS_KEYS` - valid KMS ARNs

## Error Handling

S3 exceptions: `NoSuchBucketException`, `NoSuchKeyException`, `BucketAlreadyExistsException`

HTTP codes: 200, 204, 404, 409, 500

## Testing

- Unit tests: Mock dependencies, test in isolation, suffix `Test`
- Integration tests: Real AWS SDKs v1/v2, suffix `IT`
- Test independence: Each test self-contained

## Build

```bash
./mvnw clean install              # Full build
./mvnw clean install -DskipDocker # Skip Docker
./mvnw verify -pl integration-tests
./mvnw ktlint:format
```

## Constraints

- Path-style URLs only (not `bucket.localhost`)
- Presigned URLs accepted but not validated
- Self-signed SSL certificate
- KMS validation only, no encryption
- Not for production

## Common Patterns

```kotlin
// ETag
val etag = DigestUtils.md5Hex(data)

// Response
ResponseEntity.ok()
  .header("ETag", "\"$etag\"")
  .header("Last-Modified", lastModified)
  .body(data)

// Dates
Instant.now().toString() // ISO 8601
```
