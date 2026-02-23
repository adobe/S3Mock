# Agent Context for S3Mock Server Module

Core S3Mock server implementation.

## Structure

```
server/src/main/kotlin/com/adobe/testing/s3mock/
├── S3MockApplication.kt       # Spring Boot entry
├── S3MockConfiguration.kt     # Config
├── S3MockProperties.kt        # Properties binding
├── controller/                # REST endpoints (BucketController, ObjectController)
├── dto/                       # XML/JSON models (*Result, request/response)
├── service/                   # Business logic (ObjectService, etc.)
└── store/                     # Persistence (BucketStore, ObjectStore, metadata)
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
      ?: throw NoSuchBucketException(bucketName)
    return objectStore.getObject(bucket, key)
      ?: throw NoSuchKeyException(key)
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

```kotlin
@ConfigurationProperties(prefix = "com.adobe.testing.s3mock.store")
data class StoreProperties(
  val root: Path,
  val retainFilesOnExit: Boolean = false,
  val validKmsKeys: Set<String> = emptySet(),
  val initialBuckets: Set<String> = emptySet(),
  val region: Region = Region.US_EAST_1
)
```

## Running

```bash
./mvnw spring-boot:run -pl server
./mvnw package -pl server -am && java -jar server/target/s3mock-*.jar
docker run -p 9090:9090 -p 9191:9191 adobe/s3mock:latest
```
