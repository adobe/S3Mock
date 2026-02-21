# Agent Context for S3Mock Server Module

This module contains the core S3Mock server implementation.

## Module Structure

```
server/
├── src/main/kotlin/
│   └── com/adobe/testing/s3mock/
│       ├── S3MockApplication.kt      # Spring Boot application entry point
│       ├── S3MockConfiguration.kt    # Main configuration
│       ├── S3MockProperties.kt       # Configuration properties
│       ├── controller/               # REST API endpoints
│       │   ├── BucketController.kt   # Bucket operations
│       │   ├── ObjectController.kt   # Object operations
│       │   └── *Converter.kt         # Header/parameter converters
│       ├── dto/                      # Data Transfer Objects
│       │   ├── *Result.kt            # Response objects
│       │   └── *.kt                  # Request/response models
│       ├── service/                  # Business logic
│       │   └── ObjectService.kt      # Object operations service
│       └── store/                    # Persistence layer
│           ├── BucketStore.kt        # Bucket storage
│           ├── ObjectStore.kt        # Object storage
│           ├── BucketMetadata.kt     # Bucket metadata model
│           └── S3ObjectMetadata.kt   # Object metadata model
└── src/test/                         # Unit tests
```

## Implementation Workflow

When implementing new S3 operations:

1. **Define DTOs** (`dto/`)
   - Create request/response data classes
   - Add Jackson XML annotations
   - Match AWS S3 XML structure exactly

2. **Update Store Layer** (`store/`)
   - Add persistence methods to `BucketStore` or `ObjectStore`
   - Update metadata classes if needed
   - Handle file system operations

3. **Implement Service** (`service/`)
   - Add business logic methods
   - Validate inputs
   - Use store layer for data access
   - Handle errors with S3 exceptions

4. **Add Controller Endpoint** (`controller/`)
   - Map HTTP method and path
   - Handle headers and parameters
   - Call service layer
   - Return proper response with headers

## DTO Patterns

All DTOs must serialize to XML matching AWS S3 API:

```kotlin
@JacksonXmlRootElement(localName = "CreateBucketConfiguration")
data class CreateBucketConfiguration(
  @JacksonXmlProperty(localName = "LocationConstraint")
  val locationConstraint: LocationConstraint? = null,

  @JacksonXmlProperty(localName = "Bucket")
  val bucket: BucketInfo? = null
)
```

### Common Annotations

- `@JacksonXmlRootElement` - Root element name
- `@JacksonXmlProperty` - Element/attribute name
- `@JacksonXmlElementWrapper(useWrapping = false)` - Unwrap collections
- Custom serializers/deserializers for special cases (Region, Instant, etc.)

## Store Layer Patterns

The store layer manages file system persistence:

```kotlin
@Component
class ObjectStore(
  private val storeProperties: StoreProperties
) {
  fun storeObject(bucket: BucketMetadata, key: String, data: InputStream): S3Object {
    val uuid = UUID.randomUUID().toString()
    val objectPath = getObjectPath(bucket, uuid)
    Files.createDirectories(objectPath)

    // Store binary data
    val dataPath = objectPath.resolve("binaryData")
    Files.copy(data, dataPath, StandardCopyOption.REPLACE_EXISTING)

    // Store metadata
    val metadata = S3ObjectMetadata(...)
    storeMetadata(objectPath, metadata)

    return S3Object(...)
  }
}
```

### Key Responsibilities

- File system path resolution
- Binary data storage
- Metadata serialization/deserialization
- Cleanup and deletion

## Service Layer Patterns

Business logic and S3 operation implementation:

```kotlin
@Service
class ObjectService(
  private val bucketStore: BucketStore,
  private val objectStore: ObjectStore
) {
  fun getObject(bucketName: String, key: String): S3Object {
    // Validate bucket exists
    val bucket = bucketStore.getBucketMetadata(bucketName)
      ?: throw NoSuchBucketException(bucketName)

    // Get object
    val s3Object = objectStore.getObject(bucket, key)
      ?: throw NoSuchKeyException(key)

    return s3Object
  }
}
```

### Key Responsibilities

- Input validation
- Error handling with S3 exceptions
- Coordinate store operations
- Implement S3 operation logic

## Controller Layer Patterns

REST endpoints implementing S3 API:

```kotlin
@RestController
@RequestMapping("\${com.adobe.testing.s3mock.controller.context-path:}")
class ObjectController(
  private val objectService: ObjectService
) {
  @GetMapping(
    value = ["/{bucketName:.+}/{*key}"],
    produces = [MediaType.APPLICATION_OCTET_STREAM_VALUE]
  )
  fun getObject(
    @PathVariable bucketName: String,
    @PathVariable key: String,
    @RequestHeader(required = false) headers: Map<String, String>
  ): ResponseEntity<StreamingResponseBody> {
    val s3Object = objectService.getObject(bucketName, key)

    return ResponseEntity
      .ok()
      .header("ETag", "\"${s3Object.etag}\"")
      .header("Content-Type", s3Object.contentType)
      .header("Last-Modified", s3Object.lastModified)
      .body(s3Object.dataStream)
  }
}
```

### Key Responsibilities

- HTTP request mapping
- Header and parameter parsing
- Response construction with proper headers
- Stream large responses when needed

## Testing

### Unit Tests

Test individual components with mocked dependencies:

```kotlin
@ExtendWith(MockitoExtension::class)
internal class ObjectServiceTest {
  @Mock
  private lateinit var bucketStore: BucketStore

  @Mock
  private lateinit var objectStore: ObjectStore

  @InjectMocks
  private lateinit var objectService: ObjectService

  @Test
  fun `should get object`() {
    val bucket = BucketMetadata(...)
    val s3Object = S3Object(...)

    whenever(bucketStore.getBucketMetadata("bucket")).thenReturn(bucket)
    whenever(objectStore.getObject(bucket, "key")).thenReturn(s3Object)

    val result = objectService.getObject("bucket", "key")

    assertThat(result).isEqualTo(s3Object)
  }
}
```

## Configuration

Configuration bound from environment variables:

```kotlin
@ConfigurationProperties(prefix = "com.adobe.testing.s3mock.store")
data class StoreProperties(
  val root: Path = Files.createTempDirectory("s3mock"),
  val retainFilesOnExit: Boolean = false,
  val validKmsKeys: Set<String> = emptySet(),
  val initialBuckets: Set<String> = emptySet(),
  val region: Region = Region.US_EAST_1
)
```

## Running the Server

```bash
# As Spring Boot application
./mvnw spring-boot:run -pl server

# Build executable JAR
./mvnw package -pl server -am
java -jar server/target/s3mock-*.jar

# With Docker
./mvnw clean package -pl server -am -DskipTests
docker run -p 9090:9090 -p 9191:9191 adobe/s3mock:latest
```

## Common Tasks

### Adding a New S3 Operation

1. Check AWS S3 API documentation
2. Create request/response DTOs in `dto/`
3. Add store methods in `store/` if needed
4. Implement service method in `service/`
5. Add controller endpoint in `controller/`
6. Write unit tests
7. Test with integration tests (see `integration-tests/AGENTS.md`)

### Updating Existing Operations

1. Identify affected layers (controller, service, store)
2. Update DTOs if request/response changed
3. Modify service logic
4. Update store operations if persistence changed
5. Update tests
6. Verify with integration tests

## References

- Main project context: `/AGENTS.md`
- Integration testing: `/integration-tests/AGENTS.md`
- AWS S3 API: https://docs.aws.amazon.com/AmazonS3/latest/API/
