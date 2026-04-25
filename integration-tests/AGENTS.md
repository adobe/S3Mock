# Agent Context for S3Mock Integration Tests

> Inherits all conventions from the [root AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Integration tests verifying S3Mock with real AWS SDK v2 clients. See **[docs/TESTING.md](../docs/TESTING.md)** for the full testing strategy, conventions, patterns, and running commands.

## Structure

```
integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/
├── S3TestBase.kt           # Base class with s3Client (v2)
├── *IT.kt                  # Test classes (BucketIT, ObjectIT, MultipartUploadIT, etc.)
```

## Base Class

Extend `S3TestBase` for access to:
- `s3Client` — AWS SDK v2
- `serviceEndpoint`, `serviceEndpointHttp`, `serviceEndpointHttps`

## Module-Specific Rules

- Extend **`S3TestBase`** for all integration tests
- Accept **`testInfo: TestInfo`** parameter in test methods for unique resource naming
- Use **`givenBucket(testInfo)`** for bucket creation — don't create your own helper
- Verify HTTP codes, headers (ETag, Content-Type), XML/JSON bodies, and error responses
- DON'T mock AWS SDK clients — use actual SDK clients against S3Mock

