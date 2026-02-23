# Agent Context for S3Mock Server Module

> Inherits all conventions from the [root AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Core S3Mock server implementation.

## Structure

```
server/src/main/kotlin/com/adobe/testing/s3mock/
├── S3MockApplication.kt       # Spring Boot entry
├── S3MockConfiguration.kt     # Top-level config
├── S3MockProperties.kt        # Properties binding
├── controller/
│   ├── *Controller.kt         # REST endpoints
│   ├── ControllerConfiguration.kt  # Controller beans + exception handlers
│   └── ControllerProperties.kt
├── dto/                       # XML/JSON models (*Result, request/response)
├── service/
│   ├── *Service.kt            # Business logic
│   └── ServiceConfiguration.kt  # Service beans (used in @SpringBootTest)
├── store/
│   ├── *Store.kt              # Persistence
│   ├── StoreConfiguration.kt  # Store beans (used in @SpringBootTest)
│   └── StoreProperties.kt
└── util/
    ├── DigestUtil.kt          # ETag/checksum computation (replaces Apache Commons)
    ├── EtagUtil.kt            # ETag normalization
    ├── HeaderUtil.kt          # HTTP header helpers
    └── AwsHttpHeaders.kt      # AWS-specific header constants
```

## Implementation Flow

**Adding S3 operation**: Follow **DTO → Store → Service → Controller**:

1. **DTO** (`dto/`): Data classes with Jackson XML annotations (`@JacksonXmlRootElement`, `@JacksonXmlProperty`, `@JacksonXmlElementWrapper(useWrapping = false)`). Verify element names against [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
2. **Store** (`store/`): Filesystem path resolution, binary storage, metadata JSON. Key classes: `BucketStore`, `ObjectStore`, `BucketMetadata`, `S3ObjectMetadata`.
3. **Service** (`service/`): Validation, store coordination. Throw **`S3Exception` constants** (e.g., `S3Exception.NO_SUCH_BUCKET`) — don't create new exception classes.
4. **Controller** (`controller/`): HTTP mapping only — delegate all logic to services. Controllers never catch exceptions.

## Error Handling

- `S3MockExceptionHandler` converts `S3Exception` → XML `ErrorResponse` with the correct HTTP status
- `IllegalStateExceptionHandler` converts unexpected errors → `500 InternalError`
- Add new error types as constants in `S3Exception` — DON'T create new exception classes

## Testing

Service and store unit tests use `@SpringBootTest` with `@MockitoBean`, while controller tests are slice tests using `@WebMvcTest` with `@MockitoBean` and `BaseControllerTest`. Extend the appropriate base class (`ServiceTestBase`, `StoreTestBase`, `BaseControllerTest`):

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

## Configuration

Three `@ConfigurationProperties` classes bind environment variables to typed properties:
- `StoreProperties` (`com.adobe.testing.s3mock.store.*`) — storage root, buckets, KMS, region
- `ControllerProperties` (`com.adobe.testing.s3mock.controller.*`) — context path
- `S3MockProperties` (`com.adobe.testing.s3mock.*`) — top-level settings

## Running

```bash
./mvnw spring-boot:run -pl server
docker run -p 9090:9090 -p 9191:9191 adobe/s3mock:latest
```
