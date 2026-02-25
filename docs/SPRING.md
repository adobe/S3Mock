# Spring Guidelines — S3Mock

Canonical reference for Spring Boot idioms, patterns, and code quality standards used across this project.

## Bean Registration

Register beans explicitly in `@Configuration` classes using `@Bean` factory methods — not via component scanning or class-level `@Service`/`@Component` annotations.

```kotlin
@Configuration
class ServiceConfiguration {
  @Bean
  fun bucketService(bucketStore: BucketStore, objectStore: ObjectStore): BucketService =
    BucketService(bucketStore, objectStore)
}
```

Three configuration layers mirror the architecture:
- `StoreConfiguration` — store beans
- `ServiceConfiguration` — service beans (imported in `@SpringBootTest`)
- `ControllerConfiguration` — controller, filter, and exception-handler beans

## Dependency Injection

Always use **constructor injection**. Never use `@Autowired` on fields or setters in production code.

```kotlin
// DO — constructor injection
class BucketService(
  private val bucketStore: BucketStore,
  private val objectStore: ObjectStore,
)

// DON'T — field injection
class BucketService {
  @Autowired private lateinit var bucketStore: BucketStore
}
```

## Controllers

- `@RestController` classes map HTTP only — never contain business logic
- All logic is delegated to a `@Service`; controllers call the service and return the result
- Return typed DTOs, never raw strings
- Controllers never catch exceptions — exception handlers in `ControllerConfiguration` do that

```kotlin
// DO
@GetMapping("/{bucketName}")
fun getBucket(@PathVariable bucketName: String): ResponseEntity<ListBucketResult> =
  ResponseEntity.ok(bucketService.listObjects(bucketName))

// DON'T
@GetMapping("/{bucketName}")
fun getBucket(@PathVariable bucketName: String): String {
  // business logic here ...
  return "<ListBucketResult>...</ListBucketResult>"
}
```

## Configuration Properties

Bind configuration via `@ConfigurationProperties` data classes — never inject individual values with `@Value` in production code.

```kotlin
@JvmRecord
@ConfigurationProperties("com.adobe.testing.s3mock.store")
data class StoreProperties(
  @param:DefaultValue("false") val retainFilesOnExit: Boolean,
  @param:DefaultValue("us-east-1") val region: Region,
)
```

Enable each properties class with `@EnableConfigurationProperties` in the matching `@Configuration`:

```kotlin
@Configuration
@EnableConfigurationProperties(StoreProperties::class)
class StoreConfiguration { ... }
```

## Exception Handling

- Services throw `S3Exception` constants (e.g., `S3Exception.NO_SUCH_BUCKET`) — never create new exception classes
- Exception handlers are `@ControllerAdvice` classes registered as `@Bean`s in `ControllerConfiguration`
- `S3MockExceptionHandler` converts `S3Exception` → XML `ErrorResponse` with the correct HTTP status
- `IllegalStateExceptionHandler` converts unexpected errors → `500 InternalError`

```kotlin
@ControllerAdvice
class S3MockExceptionHandler : ResponseEntityExceptionHandler() {
  @ExceptionHandler(S3Exception::class)
  fun handleS3Exception(s3Exception: S3Exception): ResponseEntity<ErrorResponse> { ... }
}
```

## Testing

### Service and Store Tests — `@SpringBootTest`

Use `@SpringBootTest` scoped to the relevant `@Configuration` class with `@MockitoBean` for dependencies.

```kotlin
@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(types = [MultipartService::class, MultipartStore::class])
internal class BucketServiceTest : ServiceTestBase() {
  @Autowired private lateinit var iut: BucketService

  @Test
  fun `should return no such bucket`() { ... }
}
```

Always extend the appropriate base class:
- `ServiceTestBase` — service layer tests
- `StoreTestBase` — store layer tests

### Controller Tests — `@WebMvcTest`

Use `@WebMvcTest` scoped to the controller under test with `@MockitoBean` for services and `BaseControllerTest` for shared fixtures.

```kotlin
@WebMvcTest(controllers = [BucketController::class], ...
@MockitoBean(types = [BucketService::class])
internal class BucketControllerTest : BaseControllerTest() {
  @Autowired private lateinit var mockMvc: MockMvc
  @Autowired private lateinit var bucketService: BucketService

  @Test
  fun `should list buckets`() { ... }
}
```

### Common Anti-Patterns

| Anti-Pattern | Refactor To |
|---|---|
| `@ExtendWith(MockitoExtension::class)` + `@Mock` + `@InjectMocks` | `@SpringBootTest` + `@MockitoBean` + `@Autowired` |
| `@Autowired` field injection in production code | Constructor injection |
| Business logic in controller method | Delegate to `@Service` |
| Returning a raw `String` from a controller | Return a typed DTO wrapped in `ResponseEntity` |
| `@Value("${property}")` scattered throughout beans | `@ConfigurationProperties` data class |
| New exception class for S3 errors | `S3Exception` constant |
