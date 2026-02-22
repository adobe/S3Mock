# Agent Context for S3Mock Test Support

Test framework integrations for using S3Mock: JUnit 4/5, Testcontainers, TestNG.

## Framework Selection

- **JUnit 5** (`junit5/`) - Modern extension with parameter injection. Default for new projects.
- **JUnit 4** (`junit4/`) - Legacy Rule API for older projects.
- **Testcontainers** (`testcontainers/`) - Docker container isolation, works with any framework.
- **TestNG** (`testng/`) - Listener-based integration for TestNG projects.
- **Common** (`common/`) - Shared `S3MockStarter` base class and utilities.

## JUnit 5 Extension

```kotlin
@ExtendWith(S3MockExtension::class)
class MyTest {
  @Test
  fun test(s3Client: S3Client) {
    s3Client.createBucket { it.bucket("test-bucket") }
  }
}
```

Builder alternative:
```kotlin
@RegisterExtension
val s3Mock = S3MockExtension.builder()
  .withInitialBuckets("bucket1")
  .withValidKmsKeys("arn:aws:kms:...")
  .build()
```

## JUnit 4 Rule

```java
@Rule
public S3MockRule s3MockRule = S3MockRule.builder()
  .withInitialBuckets("test-bucket")
  .build();
```

## Testcontainers

```kotlin
@Container
val s3Mock = S3MockContainer("latest")
  .withInitialBuckets("test-bucket")
  .withValidKmsKeys("key-id")

// Access via s3Mock.httpEndpoint or s3Mock.httpsEndpoint
```

Configuration methods: `withInitialBuckets()`, `withValidKmsKeys()`, `withRetainFilesOnExit()`, `withRoot()`, `withRegion()`.

## TestNG Listener

Configure in `testng.xml`:
```xml
<listener class-name="com.adobe.testing.s3mock.testng.S3MockListener"/>
```

Access via system properties:
```kotlin
val endpoint = System.getProperty("s3mock.httpEndpoint")
```

Parameters: `s3mock.httpPort` (9090), `s3mock.httpsPort` (9191), `s3mock.initialBuckets`, `s3mock.root`.

## Design Principles

1. Framework-agnostic core in `common` module
2. Simple API with sensible defaults
3. Proper lifecycle management (start before tests, stop after)
4. Resource cleanup after tests

## Common Issues

- **Not starting**: Check ports 9090/9191 availability, Docker running (Testcontainers)
- **Connection refused**: Verify S3Mock started, correct endpoint URL
- **Test interference**: Use unique bucket names (UUID), ensure test isolation
