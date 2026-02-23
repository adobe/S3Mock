# Agent Context for S3Mock Server Module

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

**Adding S3 operation**: DTO (Jackson XML) → Store (filesystem) → Service (validation, logic) → Controller (HTTP mapping)

### 1. DTOs (`dto/`)
```kotlin
@JacksonXmlRootElement(localName = "CreateBucketConfiguration")
data class CreateBucketConfiguration(
  @JacksonXmlProperty(localName = "LocationConstraint")
  val locationConstraint: LocationConstraint? = null
)
```

Key annotations: `@JacksonXmlRootElement`, `@JacksonXmlProperty`, `@JacksonXmlElementWrapper(useWrapping = false)`

### 2. Store (`store/`)
- Filesystem path resolution, binary storage, metadata JSON
- `BucketStore`, `ObjectStore` for CRUD operations
- `BucketMetadata`, `S3ObjectMetadata` models

### 3. Service (`service/`)
```kotlin
@Service
class ObjectService(
  private val bucketStore: BucketStore,
  private val objectStore: ObjectStore
) {
  fun getObject(bucketName: String, key: String): S3Object {
    val bucket = bucketStore.getBucketMetadata(bucketName)
      ?: throw S3Exception.NO_SUCH_BUCKET
    return objectStore.getObject(bucket, key)
      ?: throw S3Exception.NO_SUCH_KEY
  }
}
```

Responsibilities: Validation, S3 exceptions, coordinate stores

### 4. Controller (`controller/`)
```kotlin
@RestController
class ObjectController(private val objectService: ObjectService) {
  @GetMapping("/{bucketName:.+}/{*key}")
  fun getObject(@PathVariable bucketName: String, @PathVariable key: String) =
    objectService.getObject(bucketName, key).let {
      ResponseEntity.ok()
        .header("ETag", "\"${it.etag}\"")
        .body(it.dataStream)
    }
}
```

Responsibilities: HTTP mapping, headers, streaming responses

## Error Handling

- Services throw **`S3Exception`** constants (e.g., `S3Exception.NO_SUCH_KEY`, `S3Exception.INVALID_BUCKET_NAME`)
- Controllers never catch exceptions — Spring's `@ExceptionHandler` in `ControllerConfiguration` handles them
- `S3MockExceptionHandler` converts `S3Exception` → XML `ErrorResponse` with the correct HTTP status
- `IllegalStateExceptionHandler` converts unexpected errors → `500 InternalError`
- DON'T create new exception classes — add new constants to `S3Exception`

## DO / DON'T

### DO
- Follow the **DTO → Store → Service → Controller** flow when adding new S3 operations
- Add **Jackson XML annotations** matching the AWS API naming exactly (verify against [AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html))
- Use **`@SpringBootTest`** with **`@MockitoBean`** for all tests
- Use **backtick test names**: `` fun `should return object with correct etag`() ``
- Throw **`S3Exception` constants** (e.g., `S3Exception.NO_SUCH_BUCKET`, `S3Exception.NO_SUCH_KEY`) from the Service layer

### DON'T
- DON'T put business logic in controllers — controllers only map HTTP requests and delegate to services
- DON'T use `@Autowired` in production code — use constructor injection
- DON'T return raw strings — use typed DTOs for XML/JSON responses
- DON'T use `@ExtendWith(MockitoExtension::class)`, `@Mock`, or `@InjectMocks` — use `@SpringBootTest` with `@MockitoBean`
- DON'T use legacy `testSomething` naming — refactor to backtick style when touching tests

## Testing

Spring Boot tests with `@MockitoBean`:
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

Spring Boot relaxed binding maps properties to environment variables automatically:
`com.adobe.testing.s3mock.store.initial-buckets` → `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS`

```kotlin
@JvmRecord
@ConfigurationProperties("com.adobe.testing.s3mock.store")
data class StoreProperties(
  @param:DefaultValue("false") val retainFilesOnExit: Boolean,
  @param:DefaultValue("") val root: String,
  @param:DefaultValue("") val validKmsKeys: Set<String>,
  @param:DefaultValue("") val initialBuckets: List<String>,
  @param:DefaultValue("us-east-1") val region: Region
)
```

## Running

```bash
./mvnw spring-boot:run -pl server
./mvnw package -pl server -am && java -jar server/target/s3mock-*.jar
docker run -p 9090:9090 -p 9191:9191 adobe/s3mock:latest
```
