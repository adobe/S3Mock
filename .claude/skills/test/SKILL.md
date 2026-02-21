---
name: test
description: Write, update, or fix tests for S3Mock including unit tests, integration tests, and test configurations. Use when asked to test code, create test cases, or fix failing tests.
---

# Test Skill for S3Mock

This skill helps create and maintain comprehensive tests for the S3Mock project.

## When to Use

- Writing new unit or integration tests
- Fixing failing tests
- Adding test coverage for new features
- Updating tests after code changes
- Creating test data and fixtures
- Debugging test failures

## Instructions

When writing tests for S3Mock, follow these steps:

1. **Identify Test Type**
   - **Unit Tests**: Test individual components in isolation (`server/src/test/`)
   - **Integration Tests**: Test S3Mock with real AWS SDK clients (`integration-tests/src/test/`)
   - **Support Tests**: Test JUnit5, TestNG, Testcontainers integrations (`testsupport/*/src/test/`)

2. **Understand Test Context**
   - Locate relevant source code to test
   - Review existing test patterns in the module
   - Identify test framework (JUnit 5, TestNG, Mockito)
   - Determine if S3Mock instance is needed

3. **Test Structure for Integration Tests**

   ```kotlin
   // Extend S3TestBase for S3Mock setup
   class MyFeatureIT : S3TestBase() {

     @Test
     fun `should perform S3 operation`() {
       // Arrange - setup buckets, objects, test data
       val bucketName = "test-bucket"
       s3Client.createBucket(bucketName)

       // Act - perform the operation
       val result = s3Client.someOperation(bucketName, "key")

       // Assert - verify behavior
       assertThat(result).isNotNull()
     }
   }
   ```

4. **Test Structure for Unit Tests**

   ```kotlin
   @ExtendWith(MockitoExtension::class)
   internal class MyServiceTest {

     @Mock
     private lateinit var dependency: SomeDependency

     @InjectMocks
     private lateinit var service: MyService

     @Test
     fun `should handle valid input`() {
       // Arrange
       whenever(dependency.method()).thenReturn("result")

       // Act
       val result = service.doSomething()

       // Assert
       assertThat(result).isEqualTo("expected")
       verify(dependency).method()
     }
   }
   ```

5. **Test Naming Conventions**
   - Integration tests: End with `IT` (e.g., `BucketIT.kt`)
   - Unit tests: End with `Test` (e.g., `BucketServiceTest.kt`)
   - Use descriptive test names with backticks in Kotlin
   - Format: `` `should <expected behavior> when <condition>` ``

6. **S3Mock Test Setup**

   **For Integration Tests**:
   ```kotlin
   // Tests extend S3TestBase which provides:
   // - s3Client: S3Client (AWS SDK v2)
   // - s3ClientV1: AmazonS3 (AWS SDK v1)
   // - httpClient: Configured HTTP client
   // - serviceEndpoint: S3Mock endpoint URL
   ```

   **For JUnit 5 Extensions**:
   ```kotlin
   @ExtendWith(S3MockExtension::class)
   class MyTest {
     @Test
     fun test(s3Client: S3Client) {
       // S3Client injected automatically
     }
   }
   ```

   **For Testcontainers**:
   ```kotlin
   val s3MockContainer = S3MockContainer("latest")
     .withInitialBuckets("test-bucket")
   s3MockContainer.start()
   ```

7. **Common Test Patterns**

   **Testing Object Operations**:
   ```kotlin
   @Test
   fun `should store and retrieve object`() {
     val bucketName = "test-bucket"
     val key = "test-key"
     val content = "test content"

     s3Client.createBucket(bucketName)
     s3Client.putObject(bucketName, key, content.toByteArray())

     val response = s3Client.getObject(bucketName, key)
     val retrieved = response.readAllBytes().decodeToString()

     assertThat(retrieved).isEqualTo(content)
   }
   ```

   **Testing Bucket Operations**:
   ```kotlin
   @Test
   fun `should create and list buckets`() {
     val bucketName = "my-bucket"

     s3Client.createBucket(bucketName)
     val buckets = s3Client.listBuckets()

     assertThat(buckets).anyMatch { it.name() == bucketName }
   }
   ```

   **Testing Multipart Uploads**:
   ```kotlin
   @Test
   fun `should complete multipart upload`() {
     val bucketName = "test-bucket"
     val key = "large-file"

     s3Client.createBucket(bucketName)

     val uploadId = s3Client.createMultipartUpload(
       CreateMultipartUploadRequest.builder()
         .bucket(bucketName)
         .key(key)
         .build()
     ).uploadId()

     val parts = listOf(
       uploadPart(bucketName, key, uploadId, 1, "part1".toByteArray()),
       uploadPart(bucketName, key, uploadId, 2, "part2".toByteArray())
     )

     s3Client.completeMultipartUpload(
       CompleteMultipartUploadRequest.builder()
         .bucket(bucketName)
         .key(key)
         .uploadId(uploadId)
         .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
         .build()
     )

     val obj = s3Client.getObject(bucketName, key)
     assertThat(obj.readAllBytes().decodeToString()).isEqualTo("part1part2")
   }
   ```

   **Testing Error Cases**:
   ```kotlin
   @Test
   fun `should throw exception for non-existent bucket`() {
     assertThrows<NoSuchBucketException> {
       s3Client.getObject("non-existent-bucket", "key")
     }
   }
   ```

8. **Assertions and Matchers**
   - Use AssertJ: `assertThat(value).isEqualTo(expected)`
   - Use XMLUnit for XML: `assertThat(xml).and(expected).areIdentical()`
   - Use Kotlin test assertions: `assertThrows<Exception> { ... }`
   - Verify mock interactions: `verify(mock).method()`

9. **Test Data Management**
   - Create test data in test setup
   - Use meaningful test data that reflects real usage
   - Clean up resources (usually automatic with test lifecycle)
   - Use random bucket names if needed: `"bucket-${UUID.randomUUID()}"`

10. **Testing Best Practices**
    - **Isolation**: Each test should be independent
    - **Clarity**: Test one thing per test method
    - **Speed**: Keep tests fast, prefer unit tests
    - **Coverage**: Test happy path and error cases
    - **Readability**: Use descriptive names and clear assertions
    - **Maintenance**: Update tests with code changes

11. **Running Tests**

    ```bash
    # Run all tests
    ./mvnw clean verify

    # Run unit tests only
    ./mvnw test

    # Run integration tests
    ./mvnw verify -pl integration-tests

    # Run specific test
    ./mvnw test -Dtest=BucketIT

    # Skip Docker build
    ./mvnw verify -DskipDocker
    ```

12. **Test Quality Checklist**
    - [ ] Test name clearly describes what is tested
    - [ ] Arrange-Act-Assert structure followed
    - [ ] Test is independent and repeatable
    - [ ] Both success and failure cases covered
    - [ ] Assertions are specific and meaningful
    - [ ] No hard-coded waits or sleeps
    - [ ] Resources properly managed
    - [ ] Test runs successfully locally

## Project-Specific Guidelines

### S3Mock Test Architecture

- **Base Classes**: `S3TestBase` provides S3Client setup
- **Test Location**:
  - Unit: `server/src/test/kotlin/com/adobe/testing/s3mock/`
  - Integration: `integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/`
- **Languages**: Tests written in Kotlin
- **Frameworks**: JUnit 5, Mockito, AssertJ
- **Execution**: Parallel test execution enabled

### Testing S3 Operations

When testing S3 operations:
1. Verify response structure matches S3 API
2. Check HTTP status codes
3. Validate XML/JSON response bodies
4. Test with both SDK v1 and v2 if applicable
5. Verify headers (ETag, Content-Type, etc.)
6. Test presigned URLs if relevant

### Testing File System Integration

When testing storage:
1. Verify files created in correct location
2. Check metadata serialization
3. Validate bucket and object UUIDs
4. Test versioning if enabled
5. Verify cleanup on shutdown (if configured)

### Mock Configuration in Tests

```kotlin
// Configure test-specific settings
@TestPropertySource(
  properties = [
    "com.adobe.testing.s3mock.store.retainFilesOnExit=false",
    "com.adobe.testing.s3mock.store.validKmsKeys=test-key-id"
  ]
)
```

## Common Issues and Solutions

**Issue**: Test fails with "Connection refused"
- **Solution**: Ensure S3Mock is started before test runs, check port configuration

**Issue**: Test fails intermittently
- **Solution**: Check for race conditions, ensure proper test isolation

**Issue**: Test can't find bucket
- **Solution**: Verify bucket creation in test setup, check bucket name

**Issue**: XML comparison fails
- **Solution**: Use XMLUnit's `areIdentical()` ignoring whitespace and comments

## Resources

- **Test Base**: `integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/S3TestBase.kt`
- **Examples**: All `*IT.kt` files in `integration-tests/src/test/`
- **SDK Usage**: AWS SDK v2 documentation
- **Assertions**: AssertJ documentation
- **Mocking**: Mockito-Kotlin documentation
