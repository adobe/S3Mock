# Agent Context for S3Mock

This file provides context-specific information for AI agents working on the S3Mock project.

## Project Overview

**S3Mock** is a lightweight server implementing parts of the Amazon S3 API for local integration testing.

- **Language**: Kotlin 2.3.0 (with Java interoperability, target JVM 17)
- **Framework**: Spring Boot 4.0.x
- **Build Tool**: Maven 3.9+
- **Testing**: JUnit 5, Mockito, AssertJ, Testcontainers
- **Container**: Docker (Alpine Linux base)

## Project Structure

```
/
├── server/              # Core S3Mock implementation
│   ├── src/main/kotlin/ # Application code
│   └── src/test/        # Unit tests
├── integration-tests/   # Integration tests with AWS SDKs
├── testsupport/         # Test framework integrations
│   ├── junit4/
│   ├── junit5/
│   ├── testcontainers/
│   └── testng/
└── docker/              # Docker image build
```

## Architecture

S3Mock follows a layered architecture:

```
Controller Layer (HTTP endpoints, REST API)
    ↓
Service Layer (Business logic, S3 operations)
    ↓
Store Layer (File system persistence, metadata)
```

### Key Packages

- `controller/` - REST API endpoints, request/response handling
- `service/` - Business logic for S3 operations
- `store/` - File system storage and metadata management
- `dto/` - Data Transfer Objects with XML/JSON serialization

## Code Style

### Language Conventions

- **Primary**: Kotlin with idiomatic patterns
- **Naming**: PascalCase for classes, camelCase for functions/properties
- **Data Classes**: Use for DTOs and value objects
- **Null Safety**: Leverage Kotlin's type system
- **Expression Bodies**: Use for simple functions

### Spring Conventions

- Constructor injection (preferred over field injection)
- `@Service` for service layer
- `@RestController` for controllers
- `@Component` for other managed beans

### Example

```kotlin
@RestController
@RequestMapping("\${com.adobe.testing.s3mock.controller.context-path:}")
class ObjectController(
  private val objectService: ObjectService
) {
  @GetMapping("/{bucketName:.+}/{*key}")
  fun getObject(
    @PathVariable bucketName: String,
    @PathVariable key: String
  ): ResponseEntity<ByteArray> {
    val result = objectService.getObject(bucketName, key)
    return ResponseEntity.ok()
      .header("ETag", result.etag)
      .body(result.data)
  }
}
```

## XML Serialization

S3Mock uses Jackson XML for AWS S3 API compatibility. Follow AWS XML structure exactly.

```kotlin
@JacksonXmlRootElement(localName = "ListBucketResult")
data class ListBucketResult(
  @JacksonXmlProperty(localName = "Name")
  val name: String,

  @JacksonXmlElementWrapper(useWrapping = false)
  @JacksonXmlProperty(localName = "Contents")
  val contents: List<S3Object> = emptyList()
)
```

## File System Structure

S3Mock persists data to the file system:

```
/<root>/
  /<bucket-name>/
    bucketMetadata.json
    /<object-uuid>/
      binaryData
      objectMetadata.json

# With versioning:
      /<version-id>-binaryData
      /<version-id>-objectMetadata.json

# Multipart uploads:
    /multiparts/
      /<upload-id>/
        multipartMetadata.json
        /<part-number>.part
```

## Configuration

Environment variables (see `S3MockProperties.kt`):

- `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT` - Base directory for files
- `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT` - Keep files after shutdown
- `COM_ADOBE_TESTING_S3MOCK_STORE_REGION` - AWS region (default: us-east-1)
- `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS` - Comma-separated bucket names
- `COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS` - Valid KMS key ARNs

## Error Handling

Use S3-specific exceptions:

```kotlin
throw NoSuchBucketException("Bucket does not exist: $bucketName")
throw NoSuchKeyException("Key does not exist: $key")
throw BucketAlreadyExistsException("Bucket already exists: $bucketName")
```

HTTP status codes: 200 OK, 204 No Content, 404 Not Found, 409 Conflict, 500 Internal Error

## Testing Philosophy

- **Unit Tests**: Test components in isolation, mock dependencies
- **Integration Tests**: Test with real AWS SDK clients against S3Mock
- **Test Naming**: End with `Test` (unit) or `IT` (integration)
- **Test Independence**: Each test should be self-contained

## Build Commands

```bash
# Full build with tests and Docker
./mvnw clean install

# Build without Docker
./mvnw clean install -DskipDocker

# Run unit tests only
./mvnw test

# Run integration tests
./mvnw verify -pl integration-tests

# Format code
./mvnw ktlint:format

# Check code style
./mvnw checkstyle:check
```

## AWS S3 API Reference

When implementing S3 operations, refer to:
- [AWS S3 API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- README.md for list of supported operations

## Important Constraints

- **Path-style access only** (no domain-style: `http://bucket.localhost`)
- **Presigned URLs**: Accepted but parameters ignored (no validation)
- **Self-signed SSL**: Included certificate, clients must trust it
- **Mock implementation**: Not for production use
- **KMS**: Validation only, no actual encryption

## Common Patterns

### ETag Generation
```kotlin
val etag = DigestUtils.md5Hex(data)
```

### Response Headers
```kotlin
ResponseEntity.ok()
  .header("ETag", "\"$etag\"")
  .header("Last-Modified", lastModified)
  .header("Content-Type", contentType)
  .body(data)
```

### Date Formatting
```kotlin
val formatted = Instant.now().toString() // ISO 8601
```

## Documentation

- **KDoc**: Use for public APIs
- **README**: Update supported operations table when adding features
- **CHANGELOG**: Document changes in releases

## Contributing

See `.github/CONTRIBUTING.md` for contribution guidelines.
