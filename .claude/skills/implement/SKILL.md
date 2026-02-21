---
name: implement
description: Implement new features, fix bugs, or refactor code in S3Mock. Use when asked to add S3 operations, modify existing functionality, or improve code structure.
---

# Implementation Skill for S3Mock

This skill helps implement features and fix bugs in the S3Mock project following best practices.

## When to Use

- Implementing new S3 API operations
- Adding features or enhancements
- Fixing bugs and issues
- Refactoring existing code
- Optimizing performance
- Updating dependencies or configurations

## Instructions

When implementing changes to S3Mock, follow these steps:

1. **Understand the Requirement**
   - Review AWS S3 API documentation for the feature
   - Check existing similar implementations
   - Understand expected behavior and edge cases
   - Identify affected components (controller, service, store, DTO)

2. **Architecture Overview**

   S3Mock follows a layered architecture:

   ```
   Controller Layer (HTTP endpoints)
     ↓
   Service Layer (Business logic)
     ↓
   Store Layer (File system persistence)
   ```

   **Key Packages**:
   - `controller/` - REST API endpoints, request/response handling
   - `service/` - Business logic, S3 operation implementation
   - `store/` - File system storage, metadata management
   - `dto/` - Data Transfer Objects, XML/JSON serialization

3. **Implementation Workflow**

   **Step 1: Define DTOs**
   - Create data classes in `dto/` for request/response
   - Add Jackson XML annotations for serialization
   - Follow AWS S3 XML structure exactly

   ```kotlin
   @JacksonXmlRootElement(localName = "CreateBucketConfiguration")
   data class CreateBucketConfiguration(
     @JacksonXmlProperty(localName = "LocationConstraint")
     val locationConstraint: LocationConstraint? = null
   )
   ```

   **Step 2: Update Store Layer**
   - Modify `store/` classes for data persistence
   - Update `BucketMetadata` or `S3ObjectMetadata` as needed
   - Ensure proper file system structure
   - Handle versioning if applicable

   ```kotlin
   // In BucketStore or ObjectStore
   fun storeSomething(bucketName: String, data: SomeData) {
     val bucket = getBucketMetadata(bucketName)
     // Store data to file system
     Files.write(getDataPath(bucket), serialize(data))
   }
   ```

   **Step 3: Implement Service Layer**
   - Add business logic in `service/ObjectService` or create new service
   - Validate inputs and handle errors
   - Use store layer for persistence
   - Generate appropriate responses

   ```kotlin
   @Service
   class ObjectService(
     private val bucketStore: BucketStore,
     private val objectStore: ObjectStore
   ) {
     fun performOperation(bucket: String, key: String): Result {
       // Validate
       bucketStore.getBucketMetadata(bucket) ?: throw NoSuchBucketException()

       // Business logic
       val result = objectStore.doSomething(bucket, key)

       // Return
       return Result(result)
     }
   }
   ```

   **Step 4: Add Controller Endpoint**
   - Create or update controller in `controller/`
   - Map HTTP method and path
   - Handle request headers and query parameters
   - Set response headers (ETag, Content-Type, etc.)
   - Return proper HTTP status codes

   ```kotlin
   @RestController
   @RequestMapping("\${com.adobe.testing.s3mock.controller.context-path:}")
   class ObjectController(
     private val objectService: ObjectService
   ) {
     @GetMapping(
       "/{bucketName:.+}/{*key}",
       produces = [MediaType.APPLICATION_XML_VALUE]
     )
     fun getObject(
       @PathVariable bucketName: String,
       @PathVariable key: String,
       @RequestHeader headers: Map<String, String>
     ): ResponseEntity<ByteArray> {
       val result = objectService.getObject(bucketName, key)

       return ResponseEntity
         .ok()
         .header("ETag", result.etag)
         .header("Content-Type", result.contentType)
         .body(result.data)
     }
   }
   ```

4. **Code Style and Conventions**

   **Language**: Kotlin with Java interoperability

   **Naming**:
   - Classes: PascalCase
   - Functions: camelCase
   - Properties: camelCase
   - Constants: SCREAMING_SNAKE_CASE
   - Packages: lowercase

   **Kotlin Conventions**:
   ```kotlin
   // Use data classes for DTOs
   data class Result(val value: String)

   // Use nullable types appropriately
   fun find(id: String): Result?

   // Use expression bodies for simple functions
   fun isValid(value: String): Boolean = value.isNotEmpty()

   // Use named parameters for clarity
   createObject(
     bucket = "test-bucket",
     key = "test-key",
     data = byteArray
   )
   ```

   **Spring Annotations**:
   - `@Service` for service classes
   - `@RestController` for controllers
   - `@Component` for other beans
   - `@Autowired` or constructor injection (preferred)

5. **Error Handling**

   **S3 Exceptions**:
   ```kotlin
   // Use custom exceptions from com.adobe.testing.s3mock.dto
   throw NoSuchBucketException("Bucket does not exist: $bucketName")
   throw NoSuchKeyException("Key does not exist: $key")
   throw BucketAlreadyExistsException("Bucket already exists: $bucketName")
   ```

   **HTTP Status Codes**:
   - 200 OK - Successful operation
   - 204 No Content - Successful delete
   - 404 Not Found - Bucket/object not found
   - 409 Conflict - Resource conflict
   - 500 Internal Server Error - Unexpected errors

6. **XML Serialization**

   Use Jackson XML annotations:
   ```kotlin
   @JacksonXmlRootElement(localName = "ListBucketResult")
   data class ListBucketResult(
     @JacksonXmlProperty(localName = "Name")
     val name: String,

     @JacksonXmlProperty(localName = "Prefix")
     val prefix: String? = null,

     @JacksonXmlElementWrapper(useWrapping = false)
     @JacksonXmlProperty(localName = "Contents")
     val contents: List<S3Object> = emptyList()
   )
   ```

7. **Testing**

   **Write Tests**:
   - Add unit tests in `server/src/test/`
   - Add integration tests in `integration-tests/src/test/`
   - Test both success and error cases
   - Test with AWS SDK v1 and v2 if applicable

   **Test Coverage**:
   - New features: 80%+ coverage
   - Bug fixes: Add test reproducing the bug
   - Refactoring: Maintain existing coverage

8. **Documentation**

   **KDoc Comments**:
   ```kotlin
   /**
    * Retrieves an object from the specified bucket.
    *
    * @param bucketName The bucket name
    * @param key The object key
    * @return The object data and metadata
    * @throws NoSuchBucketException if bucket doesn't exist
    * @throws NoSuchKeyException if object doesn't exist
    */
   fun getObject(bucketName: String, key: String): S3Object
   ```

   **Update README**: Add to supported operations table if implementing new S3 API

9. **Build and Verify**

   ```bash
   # Format code
   ./mvnw ktlint:format

   # Run checkstyle
   ./mvnw checkstyle:check

   # Run unit tests
   ./mvnw test

   # Run integration tests
   ./mvnw verify

   # Build without Docker
   ./mvnw clean install -DskipDocker
   ```

10. **Implementation Checklist**
    - [ ] AWS S3 API documentation reviewed
    - [ ] DTOs created with proper XML annotations
    - [ ] Store layer updated for persistence
    - [ ] Service layer implements business logic
    - [ ] Controller endpoint added with proper mapping
    - [ ] Error handling implemented
    - [ ] Unit tests written
    - [ ] Integration tests written
    - [ ] Code formatted and linted
    - [ ] Documentation updated
    - [ ] Build passes locally

## Project-Specific Guidelines

### S3Mock Technology Stack

- **Language**: Kotlin 2.3.0 (target: JVM 17)
- **Framework**: Spring Boot 4.0.x
- **Build**: Maven 3.9+
- **Serialization**: Jackson XML/JSON
- **Testing**: JUnit 5, Mockito, AssertJ
- **Container**: Docker Alpine Linux

### File System Structure

Objects stored at:
```
/<root>/<bucket-name>/<object-uuid>/binaryData
/<root>/<bucket-name>/<object-uuid>/objectMetadata.json
/<root>/<bucket-name>/bucketMetadata.json
```

With versioning:
```
/<root>/<bucket-name>/<object-uuid>/<version-id>-binaryData
/<root>/<bucket-name>/<object-uuid>/<version-id>-objectMetadata.json
```

### Configuration

Use environment variables (see `S3MockProperties.kt`):
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

### Common Patterns

**ETag Generation**:
```kotlin
val etag = DigestUtils.md5Hex(data)
```

**UUID for Objects**:
```kotlin
val uuid = UUID.randomUUID().toString()
```

**Date Formatting**:
```kotlin
// ISO 8601 format for S3 API
val formatted = Instant.now().toString()
```

**Response Headers**:
```kotlin
ResponseEntity
  .ok()
  .header("ETag", "\"$etag\"")
  .header("Last-Modified", lastModified)
  .header("Content-Type", contentType)
  .header("Content-Length", size.toString())
  .body(data)
```

### Implementing New S3 Operations

1. Check AWS S3 API docs: https://docs.aws.amazon.com/AmazonS3/latest/API/
2. Find operation in table (lines 84-188 in README.md)
3. Create branch from `main` or maintenance branch
4. Follow implementation workflow above
5. Update README.md supported operations table
6. Run full test suite
7. Create PR with clear description

### Performance Considerations

- Use streaming for large files
- Avoid loading entire files into memory
- Use `Files.copy()` for file operations
- Consider parallel streams for batch operations
- Profile with large datasets if needed

### Security Considerations

- Validate all inputs (bucket names, keys, headers)
- Sanitize file paths to prevent directory traversal
- Use secure random for UUIDs
- Don't log sensitive data
- Follow Spring Security best practices

## Common Implementation Patterns

### Adding a New Bucket Operation

```kotlin
// 1. DTO (if needed)
@JacksonXmlRootElement(localName = "OperationRequest")
data class OperationRequest(...)

// 2. Service
fun performBucketOperation(bucketName: String): Result {
  val bucket = bucketStore.getBucketMetadata(bucketName)
    ?: throw NoSuchBucketException(bucketName)
  // Logic here
  return Result(...)
}

// 3. Controller
@PostMapping("/{bucketName:.+}?operation")
fun bucketOperation(
  @PathVariable bucketName: String,
  @RequestBody request: OperationRequest
): ResponseEntity<OperationResponse> {
  val result = bucketService.performBucketOperation(bucketName)
  return ResponseEntity.ok(result)
}
```

### Adding a New Object Operation

```kotlin
// Similar to bucket but with key parameter
@GetMapping("/{bucketName:.+}/{*key}")
fun objectOperation(
  @PathVariable bucketName: String,
  @PathVariable key: String
): ResponseEntity<Result> {
  val result = objectService.performOperation(bucketName, key)
  return ResponseEntity.ok(result)
}
```

## Resources

- **AWS S3 API**: https://docs.aws.amazon.com/AmazonS3/latest/API/
- **Spring Boot Docs**: https://docs.spring.io/spring-boot/docs/current/reference/
- **Kotlin Docs**: https://kotlinlang.org/docs/home.html
- **Jackson XML**: https://github.com/FasterXML/jackson-dataformat-xml
- **S3Mock Main Classes**:
  - `S3MockApplication.kt` - Application entry point
  - `ObjectController.kt` - Object operations
  - `BucketController.kt` - Bucket operations
  - `ObjectService.kt` - Object business logic
  - `BucketStore.kt` - Bucket storage
  - `ObjectStore.kt` - Object storage
