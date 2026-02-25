# Agent Context for S3Mock Test Support

> Inherits all conventions from the [root AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Test framework integrations for using S3Mock: JUnit 5, Testcontainers, TestNG. See **[docs/TESTING.md](../docs/TESTING.md)** for the overall testing strategy.

> **Deprecation Notice (6.x):** The JUnit 5, TestNG, and all direct-integration modules will be
> removed in S3Mock 6.x. Testcontainers will become the only officially supported testing approach.
> See [CHANGELOG.md](../CHANGELOG.md) for details.

## Framework Selection

- **Testcontainers** (`testcontainers/`) — Docker container isolation, works with any framework. **Recommended.**
- **JUnit 5** (`junit5/`) — Extension with parameter injection. Will be removed in 6.x.
- **TestNG** (`testng/`) — Listener-based integration. Will be removed in 6.x.
- **Common** (`common/`) — Shared `S3MockStarter` base class and utilities.

## Module-Specific Rules

- Prefer **Testcontainers** for new test setups — it provides the best isolation
- Keep framework modules **thin** — delegate to `common/` for shared logic
- Maintain backward compatibility for existing public APIs
- DON'T invest heavily in `junit5/` or `testng/` — they will be removed in 6.x
- DON'T add framework-specific logic that belongs in `common/`

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
