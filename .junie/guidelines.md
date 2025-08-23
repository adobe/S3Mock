# S3Mock Development Guidelines (Concise)

Essential info for working on S3Mock. This top section is a quick, no‑frills guide. Details follow below.

Quickstart TL;DR
- Build (fast): ./mvnw clean install -DskipDocker
- Build (full): ./mvnw clean install
- Server tests only: ./mvnw -pl server test
- All tests incl. ITs (requires Docker): ./mvnw verify
- One server test: ./mvnw -pl server test -Dtest=ObjectStoreTest
- One IT: ./mvnw -pl integration-tests -am verify -Dit.test=BucketIT

Requirements
- JDK 17+
- Docker (only for integration tests and Docker image build)

Common config (env vars)
- COM_ADOBE_TESTING_S3MOCK_STORE_REGION (default: us-east-1)
- COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS
- COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT (default: false)
- debug / trace (Spring Boot flags)

Tips
- IDE runs: use server tests; ITs need Docker via Maven lifecycle.
- Debug server on 9090/9191; trust self‑signed cert or use HTTP.
- Before declaring done: run a full Maven build successfully.

Troubleshooting
- Connection refused: ensure S3Mock is running on expected ports.
- SSL errors: trust self‑signed cert or switch to HTTP.
- Docker errors: ensure Docker is running and you have permissions.

## Junie Operations Playbook (Critical)
To ensure tests execute successfully in this environment, follow these strict rules:

- Default test scope: server module only. Do NOT run full project builds by default.
- Use the test tool, not shell, to run tests:
  - Preferred: run_test on specific test files, e.g., "server/src/test/kotlin/com/adobe/testing/s3mock/store/ObjectStoreTest.kt".
  - One test by name: use run_test with full path and the test method name parameter.
  - Note: Directory-wide runs via run_test may not be supported in this environment. If you need to run all server tests, use Maven with: ./mvnw -pl server -DskipDocker test.
- Avoid integration tests unless explicitly requested and Docker availability is confirmed. If requested, run via Maven lifecycle only.
- If a build is required, prefer fast builds:
  - Use ./mvnw -pl server -am -DskipDocker clean test or rely on run_test which compiles as needed.
  - Only run ./mvnw clean install (full) when the user explicitly asks for a full build or cross-module changes demand it.
- Never run mvnw verify without confirming Docker is available; if not available, add -DskipDocker.
- Java 17+ required; if build fails due to JDK, report and stop, do not retry with different commands.
- Decision tree:
  1) Need to validate changes in server module? -> run_test on one or more specific test files (fast path). If you truly need all server tests, use: ./mvnw -pl server -DskipDocker test.
  2) Need a specific server test? -> run_test on that file.
  3) Need ITs and Docker is confirmed? -> mvnw -pl integration-tests -am verify; otherwise skip.
  4) Need a build artifact quickly? -> mvnw clean install -DskipDocker.

Note: Always summarize which scope you ran and why.

—

## Build and Configuration Instructions

### Building the Project

S3Mock uses Maven for building. The project includes a Maven wrapper (`mvnw`) so you don't need to install Maven separately.

#### Basic Build Commands

```bash
# Build the entire project
./mvnw clean install

# Build without running Docker (useful for quick builds)
./mvnw clean install -DskipDocker

# Build a specific module
./mvnw clean install -pl server -am
```

#### Build Requirements

- JDK 17 or higher
- Docker (for building the Docker image and running integration tests)

### Configuration Options

S3Mock can be configured with the following environment variables:

| Environment Variable                                  | Legacy Name                       | Description                                           | Default             |
|-------------------------------------------------------|-----------------------------------|-------------------------------------------------------|---------------------|
| `COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS`       | `validKmsKeys`                    | List of KMS Key-Refs that are treated as valid        | none                |
| `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS`      | `initialBuckets`                  | List of bucket names that will be available initially | none                |
| `COM_ADOBE_TESTING_S3MOCK_STORE_REGION`               | `COM_ADOBE_TESTING_S3MOCK_REGION` | The region the S3Mock is supposed to mock             | `us-east-1`         |
| `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT`                 | `root`                            | Base directory for temporary files                    | Java temp directory |
| `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT` | `retainFilesOnExit`               | Set to `true` to keep files after shutdown            | `false`             |
| `debug`                                               | -                                 | Enable Spring Boot's debug output                     | `false`             |
| `trace`                                               | -                                 | Enable Spring Boot's trace output                     | `false`             |

## Testing Information

### Test Structure

The project uses JUnit 5 for testing. There are two main types of tests in the project:

1. **Integration Tests**: Located in the `integration-tests` module and written in Kotlin. These tests verify the end-to-end functionality of S3Mock by starting a Docker container and making actual HTTP requests to it.

2. **Server Module Tests**: Located in the `server` module and primarily written in Kotlin. These include:
   - Unit tests for utility classes and DTOs
   - Spring Boot tests for controllers, services, and stores
   - XML serialization/deserialization tests

#### Integration Tests

The main test base class for integration tests is `S3TestBase` which provides utility methods for:
- Creating S3 clients
- Managing buckets and objects
- Handling SSL certificates
- Generating test data

#### Server Module Tests

The server module contains several types of tests:

1. **Controller Tests**: Use `@SpringBootTest` with `WebEnvironment.RANDOM_PORT` and `TestRestTemplate` to test HTTP endpoints. These tests mock the service layer using `@MockitoBean`.

2. **Store Tests**: Use `@SpringBootTest` with `WebEnvironment.NONE` to test the data storage layer. These tests often use `@Autowired` to inject the component under test.

3. **Service Tests**: Test the service layer with mocked dependencies.

4. **Unit Tests**: Simple tests for utility classes and DTOs without Spring context.

Common base classes and utilities:
- `BaseControllerTest`: Sets up XML serialization/deserialization for controller tests
- `StoreTestBase`: Common setup for store tests
- `ServiceTestBase`: Common setup for service tests

### Running Tests

#### Running Server Module Tests

The server module tests can be run without Docker:

```bash
# Run all server module tests
./mvnw -pl server test

# Run a specific test class
./mvnw -pl server test -Dtest=ObjectStoreTest

# Run a specific test method
./mvnw -pl server test -Dtest=ObjectStoreTest#testStoreObject
```

The server module uses Spring Test Profiler to analyze test performance. Test execution times are reported in the `target/spring-test-profiler` directory.

#### Running Integration Tests

Integration tests require Docker to be running as they start an S3Mock Docker container. The integration tests can only be executed through Maven because they need the S3Mock to run. The process works as follows:

1. The Docker Maven plugin (io.fabric8:docker-maven-plugin) starts the S3Mock Docker container in the pre-integration-test phase
2. The Maven Failsafe plugin runs the integration tests against the running container
3. The Docker Maven plugin stops the container in the post-integration-test phase

```bash
# Run all integration tests
./mvnw verify

# Run a specific integration test
./mvnw -pl integration-tests -am verify -Dit.test=BucketIT

# Run tests without Docker (will skip integration tests)
./mvnw verify -DskipDocker
```

Note that attempting to run integration tests directly from your IDE without starting the S3Mock Docker container will result in connection errors, as the tests expect S3Mock to be running on specific ports.

### Writing New Tests

#### Writing Integration Tests

To create a new integration test:

1. Create a new Kotlin class in the `integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its` directory
2. Extend the `S3TestBase` class to inherit utility methods
3. Name your test class with an `IT` suffix to be recognized as an integration test
4. Use the provided S3 client methods to interact with S3Mock

Example integration test:

```kotlin
// ExampleIT.kt
internal class ExampleIT : S3TestBase() {
    private val s3Client = createS3Client()

    @Test
    fun testPutAndGetObject(testInfo: TestInfo) {
        // Create a bucket
        val bucketName = givenBucket(testInfo)
        
        // Create test content
        val content = "This is a test file content"
        val contentBytes = content.toByteArray(StandardCharsets.UTF_8)
        
        // Put an object into the bucket
        val key = "test-object.txt"
        val putObjectResponse = s3Client.putObject(
            PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            RequestBody.fromBytes(contentBytes)
        )
        
        // Verify the object was uploaded successfully
        assertThat(putObjectResponse.eTag()).isNotBlank()
        
        // Get the object back
        val getObjectResponse = s3Client.getObject(
            GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()
        )
        
        // Read the content and verify it matches what we uploaded
        val retrievedContent = getObjectResponse.readAllBytes()
        assertThat(retrievedContent).isEqualTo(contentBytes)
        
        // Clean up
        s3Client.deleteObject { it.bucket(bucketName).key(key) }
    }
}
```

#### Writing Server Module Tests

The server module uses different testing approaches depending on what's being tested:

1. **Controller Tests**:
   - Extend `BaseControllerTest` to inherit XML serialization setup
   - Use `@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)`
   - Use `@MockitoBean` to mock service dependencies
   - Inject `TestRestTemplate` to make HTTP requests to the controller

Example controller test:

```kotlin
// BucketControllerTest.kt
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@MockitoBean(classes = [BucketService::class, ObjectService::class, MultipartService::class])
internal class BucketControllerTest : BaseControllerTest() {
    @Autowired
    private lateinit var restTemplate: TestRestTemplate
    
    @MockitoBean
    private lateinit var bucketService: BucketService
    
    @Test
    fun testListBuckets() {
        // Mock service response
        whenever(bucketService.listBuckets()).thenReturn(givenBuckets(2))
        
        // Make HTTP request
        val response = restTemplate.getForEntity("/", ListAllMyBucketsResult::class.java)
        
        // Verify response
        assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
        assertThat(response.body.buckets.bucket).hasSize(2)
    }
}
```

2. **Store Tests**:
   - Extend `StoreTestBase` for common setup
   - Use `@SpringBootTest(webEnvironment = WebEnvironment.NONE)`
   - Use `@Autowired` to inject the component under test

Example store test:

```kotlin
// ObjectStoreTest.kt
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(classes = [KmsKeyStore::class, BucketStore::class])
internal class ObjectStoreTest : StoreTestBase() {
    @Autowired
    private lateinit var objectStore: ObjectStore
    
    @Test
    fun testStoreAndGetObject() {
        // Test storing and retrieving an object
        val sourceFile = File(TEST_FILE_PATH)
        val id = UUID.randomUUID().toString()
        val name = sourceFile.name
        
        objectStore.storeS3ObjectMetadata(
            metadataFrom("test-bucket"), id, name, "text/plain", 
            emptyMap(), sourceFile.toPath(), 
            emptyMap(), emptyMap(), null, emptyList(), 
            null, null, Owner.DEFAULT_OWNER, StorageClass.STANDARD
        )
        
        val metadata = objectStore.getS3ObjectMetadata(metadataFrom("test-bucket"), id, null)
        
        assertThat(metadata.key).isEqualTo(name)
        assertThat(metadata.contentType).isEqualTo("text/plain")
    }
}
```

3. **Unit Tests**:
   - Use standard JUnit 5 tests without Spring context
   - Focus on testing a single class or method in isolation

Example unit test:

```kotlin
// DigestUtilTest.kt
internal class DigestUtilTest {
    @Test
    fun testHexDigest() {
        val input = "test data".toByteArray()
        val expected = DigestUtils.md5Hex(input)
        
        assertThat(DigestUtil.hexDigest(input)).isEqualTo(expected)
    }
}
```

## Additional Development Information

### Project Structure

S3Mock is a multi-module Maven project:

- `build-config`: Build configuration files
- `docker`: Docker image build module
- `integration-tests`: Integration tests
- `server`: Core S3Mock implementation
- `testsupport`: Test support modules for different testing frameworks

### Code Style

The project uses Checkstyle for Java code style checking. The configuration is in `build-config/checkstyle.xml`.

### License Headers

S3Mock uses the Apache License 2.0 and enforces proper license headers in all source files through the `license-maven-plugin`. Important rules:

- All source files must include the proper license header
- When modifying a file, the copyright year range in the header must be updated to include the current year
- The format is: `Copyright 2017-YYYY Adobe.` where YYYY is the current year
- The license check runs automatically during the build process
- To fix license headers, run: `./mvnw license:format`
- To check license headers without modifying files: `./mvnw license:check`

### Debugging

To debug the S3Mock server:

1. Run the S3MockApplication class in your IDE with debug mode
2. The server will start on ports 9090 (HTTP) and 9191 (HTTPS)
3. Configure your S3 client to connect to these ports

Alternatively, you can run the Docker container with debug enabled:

```bash
docker run -p 9090:9090 -p 9191:9191 -e debug=true -t adobe/s3mock
```

### Common Issues

1. **Connection refused errors**: Ensure the S3Mock server is running and the ports are correctly configured.

2. **SSL certificate errors**: S3Mock uses a self-signed certificate. Configure your client to trust all certificates or use HTTP instead.

3. **Docker-related errors**: Make sure Docker is running, and you have permissions to create containers.

### Recommended Development Workflow

1. Make changes to the code
2. Validate changes with server module tests first (fast path)
   - Use the run_test tool on "server/src/test" or on a specific test file/method.
   - Prefer this over invoking Maven directly; run_test compiles as needed.
3. Only run a full Maven build when explicitly requested or when cross-module changes demand it
   - If building in this environment, prefer fast builds: ./mvnw -pl server -am -DskipDocker clean test
   - Do not run mvnw verify unless Docker is confirmed; otherwise add -DskipDocker
4. Run integration tests only when Docker availability is confirmed and when explicitly requested
   - Execute via Maven lifecycle: ./mvnw -pl integration-tests -am verify (or add -DskipDocker to skip ITs)
5. Optionally build the Docker image to verify packaging when needed
6. Test with your application to verify real-world usage
