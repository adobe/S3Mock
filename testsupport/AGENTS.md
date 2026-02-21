# Agent Context for S3Mock Test Support

This module provides test framework integrations for using S3Mock in various testing environments.

## Module Structure

```
testsupport/
├── common/           # Shared test support utilities
├── junit4/           # JUnit 4 Rule
├── junit5/           # JUnit 5 Extension
├── testcontainers/   # Testcontainers support
└── testng/           # TestNG Listener
```

## Common Module

Shared utilities used by all test framework integrations.

### Key Classes

- `S3MockStarter` - Base class for starting S3Mock programmatically
- Configuration helpers
- Common test utilities

## JUnit 5 Extension

Provides `S3MockExtension` for JUnit 5 tests.

### Usage Patterns

**Declarative with Injection**:

```kotlin
@ExtendWith(S3MockExtension::class)
class MyTest {
  @Test
  fun test(s3Client: S3Client) {
    // S3Client injected automatically
    s3Client.createBucket { it.bucket("test-bucket") }
  }
}
```

**Programmatic with Builder**:

```kotlin
class MyTest {
  @RegisterExtension
  val s3Mock = S3MockExtension.builder()
    .withInitialBuckets("bucket1", "bucket2")
    .withValidKmsKeys("arn:aws:kms:us-east-1:1234567890:key/test-key")
    .build()

  @Test
  fun test() {
    val s3Client = s3Mock.createS3ClientV2()
    // Use client
  }
}
```

### Implementation Notes

- Starts S3Mock before all tests
- Stops S3Mock after all tests
- Provides parameter injection for S3Client
- Supports both HTTP and HTTPS endpoints

## JUnit 4 Rule

Provides `S3MockRule` for JUnit 4 tests.

### Usage

```java
public class MyTest {
  @Rule
  public S3MockRule s3MockRule = S3MockRule.builder()
    .withInitialBuckets("test-bucket")
    .build();

  @Test
  public void test() {
    AmazonS3 s3Client = s3MockRule.createS3Client();
    s3Client.createBucket("my-bucket");
  }
}
```

## Testcontainers Support

Provides `S3MockContainer` - a Testcontainers implementation.

### Usage

**With JUnit 5**:

```kotlin
@Testcontainers
class MyTest {
  @Container
  val s3Mock = S3MockContainer("latest")
    .withInitialBuckets("test-bucket")
    .withValidKmsKeys("test-key-id")

  @Test
  fun test() {
    val s3Client = S3Client.builder()
      .endpointOverride(URI.create(s3Mock.httpEndpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("foo", "bar")
        )
      )
      .build()

    s3Client.createBucket { it.bucket("my-bucket") }
  }
}
```

**Manual Management**:

```kotlin
class MyTest {
  private lateinit var s3Mock: S3MockContainer

  @BeforeEach
  fun setup() {
    s3Mock = S3MockContainer("latest")
      .withInitialBuckets("test-bucket")
    s3Mock.start()
  }

  @AfterEach
  fun teardown() {
    s3Mock.stop()
  }

  @Test
  fun test() {
    // Use s3Mock.httpEndpoint or s3Mock.httpsEndpoint
  }
}
```

### Container Configuration

```kotlin
S3MockContainer("latest")
  .withInitialBuckets("bucket1", "bucket2")
  .withValidKmsKeys("key1", "key2")
  .withRetainFilesOnExit(true)
  .withRoot("/custom/path")
  .withRegion("us-west-2")
```

### Key Methods

- `httpEndpoint` - Get HTTP endpoint URL
- `httpsEndpoint` - Get HTTPS endpoint URL
- `withInitialBuckets()` - Pre-create buckets
- `withValidKmsKeys()` - Configure valid KMS keys
- `withRetainFilesOnExit()` - Keep files after container stops

## TestNG Listener

Provides `S3MockListener` for TestNG tests.

### Usage

Configure in `testng.xml`:

```xml
<!DOCTYPE suite SYSTEM "https://testng.org/testng-1.0.dtd" >
<suite name="S3Mock Test Suite">
  <listeners>
    <listener class-name="com.adobe.testing.s3mock.testng.S3MockListener"/>
  </listeners>

  <test name="S3Mock Tests">
    <parameter name="s3mock.initialBuckets" value="test-bucket"/>
    <classes>
      <class name="com.example.MyTest"/>
    </classes>
  </test>
</suite>
```

In test class:

```kotlin
class MyTest {
  @Test
  fun test() {
    val endpoint = System.getProperty("s3mock.httpEndpoint")
    val s3Client = S3Client.builder()
      .endpointOverride(URI.create(endpoint))
      // ... configure
      .build()

    s3Client.createBucket { it.bucket("my-bucket") }
  }
}
```

### Configuration Parameters

- `s3mock.httpPort` - HTTP port (default: 9090)
- `s3mock.httpsPort` - HTTPS port (default: 9191)
- `s3mock.initialBuckets` - Comma-separated bucket names
- `s3mock.root` - Storage root directory

## Writing Test Support Code

When modifying or extending test support:

### Design Principles

1. **Framework-agnostic core**: Keep common logic in `common` module
2. **Simple API**: Minimize configuration complexity
3. **Sensible defaults**: Work out of the box for common cases
4. **Lifecycle management**: Proper startup/shutdown
5. **Resource cleanup**: Clean up after tests

### Example: Adding New Configuration

```kotlin
// In common module
abstract class S3MockStarter {
  var customOption: String? = null

  protected fun buildEnvironment(): Map<String, String> {
    val env = mutableMapOf<String, String>()
    customOption?.let { env["CUSTOM_ENV_VAR"] = it }
    return env
  }
}

// In JUnit 5 extension
class S3MockExtension {
  class Builder {
    fun withCustomOption(value: String): Builder {
      this.customOption = value
      return this
    }
  }
}
```

## Testing Test Support

Each module includes tests:

```kotlin
// Test the extension itself
@ExtendWith(S3MockExtension::class)
class S3MockExtensionTest {
  @Test
  fun `extension should start S3Mock`(s3Client: S3Client) {
    // Verify S3Mock is accessible
    assertThat(s3Client.listBuckets()).isNotNull()
  }

  @Test
  fun `should create initial buckets`() {
    // Test initial bucket creation
  }
}
```

## Maven Dependencies

Users add test support as test dependencies:

**JUnit 5**:
```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-junit5</artifactId>
  <version>${s3mock.version}</version>
  <scope>test</scope>
</dependency>
```

**Testcontainers**:
```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-testcontainers</artifactId>
  <version>${s3mock.version}</version>
  <scope>test</scope>
</dependency>
```

## Common Issues

### S3Mock Not Starting

- Check port availability (9090, 9191)
- Verify Docker is running (for Testcontainers)
- Check logs for startup errors

### Connection Refused

- Ensure S3Mock started before tests run
- Verify endpoint URL is correct
- Check firewall/network settings

### Tests Interfere with Each Other

- Use unique bucket names (UUID)
- Ensure proper test isolation
- Consider separate S3Mock instances per test class

## References

- Main project context: `/AGENTS.md`
- Integration tests: `/integration-tests/AGENTS.md`
- JUnit 5: https://junit.org/junit5/
- Testcontainers: https://www.testcontainers.org/
- TestNG: https://testng.org/
