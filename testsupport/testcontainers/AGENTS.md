# Agent Context for S3Mock testsupport/testcontainers

> Inherits all conventions from the [root AGENTS.md](../../AGENTS.md) and [testsupport/AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Testcontainers integration for S3Mock. **This is the recommended approach** for new test setups and will remain the only officially supported integration after 6.x removes the JUnit 5 and TestNG modules.

## Usage

```kotlin
@Container
val s3Mock = S3MockContainer("latest")
  .withInitialBuckets("test-bucket")
  .withValidKmsKeys("arn:aws:kms:us-east-1:1234567890:key/my-key")

// Connect via HTTP (no TLS config needed):
val s3Client = S3Client.builder()
  .endpointOverride(URI.create(s3Mock.httpEndpoint))
  .build()

// Or HTTPS (must trust the self-signed certificate):
val s3Client = S3Client.builder()
  .endpointOverride(URI.create(s3Mock.httpsEndpoint))
  .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
    AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, true).build()
  ))
  .build()
```

## Property Name Format

S3MockContainer configures the container via Docker environment variables. Spring Boot inside the container automatically maps env vars to properties using its [relaxed binding](https://docs.spring.io/spring-boot/reference/features/external-config.html#features.external-config.typesafe-configuration-properties.relaxed-binding.environment-variables) rules: uppercase the Spring property key and replace `.` with `_`.

```
Spring property                                    → Docker env var
com.adobe.testing.s3mock.store.initialBuckets      → COM_ADOBE_TESTING_S3MOCK_STORE_INITIALBUCKETS
com.adobe.testing.s3mock.store.root                → COM_ADOBE_TESTING_S3MOCK_STORE_ROOT
com.adobe.testing.s3mock.store.validKmsKeys        → COM_ADOBE_TESTING_S3MOCK_STORE_VALIDKMSKEYS
com.adobe.testing.s3mock.store.region              → COM_ADOBE_TESTING_S3MOCK_STORE_REGION
com.adobe.testing.s3mock.store.retainFilesOnExit   → COM_ADOBE_TESTING_S3MOCK_STORE_RETAINFILESONEXIT
```

Both representations refer to the same underlying `StoreProperties` / `ControllerProperties` field in `server/`. `testsupport/common/` passes the Spring form directly (in-process); this module passes the env var form to Docker. They are two representations of the same property, not two independent formats.

## Footguns

**`withVolumeAsRoot(root)`**: Docker must have read/write permission on the host path. If Docker cannot access the path, the container starts but writes are silently discarded — no error is thrown. Verify Docker Desktop sharing settings before using this.

**`httpsEndpoint` requires trust-all-certificates**: `S3MockContainer` uses a self-signed SSL certificate. Any AWS SDK client connecting to `httpsEndpoint` must disable certificate validation (see usage example above). Forgetting this produces a `SSLHandshakeException` that looks like a connectivity issue.

**`httpEndpoint` vs `httpsEndpoint`**: `httpEndpoint` uses port 9090 and needs no TLS config. `httpsEndpoint` uses port 9191 and requires trust-all-certs. Both are exposed by default. Use `httpEndpoint` for simplicity in tests unless TLS behaviour is explicitly under test.

**Wait strategy**: The container waits for `GET /favicon.ico` on port 9090 to return 200. Do not change this to `/actuator/health` — that endpoint is only available when the `actuator` Spring profile is active, which it is not by default.

## Adding New Configuration

When a new property is added to `StoreProperties` or `ControllerProperties` in `server/`, derive the env var key by uppercasing the Spring property name and replacing `.` with `_`:

1. Add a `private const val PROP_X = "COM_ADOBE_TESTING_S3MOCK_..."` constant in `S3MockContainer.Companion`
2. Add a `fun withX(value: ...): S3MockContainer = withEnv(PROP_X, value.toString())` method
3. Ensure the same config is also exposed in `testsupport/common/` using the Spring property form

## Structure

```
src/main/kotlin/.../testcontainers/
└── S3MockContainer.kt    # GenericContainer subclass: config methods + endpoint accessors
```

## Ports

| Protocol | Container port | Accessor |
|---|---|---|
| HTTP | 9090 | `httpEndpoint`, `httpServerPort` |
| HTTPS | 9191 | `httpsEndpoint`, `httpsServerPort` |
