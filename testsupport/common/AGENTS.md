# Agent Context for S3Mock testsupport/common

> Inherits all conventions from the [root AGENTS.md](../../AGENTS.md) and [testsupport/AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Shared in-process starter and builder base for JUnit 5 and TestNG integrations.

> **6.x notice**: This module will be removed in 6.x along with the JUnit 5 and TestNG modules.
> Testcontainers will become the only supported approach. Do not add features here beyond what is
> needed to keep existing integrations working.

## Public API Contract

`S3MockStarter` and `BaseBuilder<T>` are library APIs consumed by external users. **Never make breaking changes** to their public method signatures, parameter types, or return types. Additions (new `withX()` methods) are safe; removals or renames are not.

## In-Process Execution

`S3MockStarter.start()` calls `S3MockApplication.start(properties)` directly, starting S3Mock **in the same JVM as the test**. This means:

- The test classpath and S3Mock's classpath are shared — version conflicts between test dependencies and S3Mock's dependencies will surface here and not in Testcontainers
- Port `0` is used by default (random) for both HTTP and HTTPS — always use `s3MockStarter.port` / `s3MockStarter.httpPort`, never hardcode 9090/9191

## Property Name Format

This module passes properties directly to the in-process `S3MockApplication`, so it uses the **Spring property key form**:

```
com.adobe.testing.s3mock.store.initialBuckets
com.adobe.testing.s3mock.store.root
com.adobe.testing.s3mock.store.validKmsKeys
com.adobe.testing.s3mock.store.region
com.adobe.testing.s3mock.store.retainFilesOnExit
```

`testsupport/testcontainers/` uses the same underlying properties but passes them as Docker environment variables — Spring Boot's relaxed binding maps `COM_ADOBE_TESTING_S3MOCK_STORE_INITIALBUCKETS` back to `com.adobe.testing.s3mock.store.initialBuckets` automatically. Both refer to the same `StoreProperties` fields in `server/`; this module just uses the Spring form directly.

Constants for Spring property keys are defined in `S3MockStarter.Companion`. Constants for `S3MockApplication` itself (ports, silent mode, SSL params) live in `S3MockApplication.Companion`.

## Adding New Configuration

When a new property is added to `StoreProperties` or `ControllerProperties` in `server/`:

1. Add a `private const val PROP_X = "com.adobe.testing.s3mock...."` constant in `S3MockStarter.Companion`
2. Add a `fun withX(value: ...): BaseBuilder<T>` method in `BaseBuilder` that sets `arguments[PROP_X] = value.toString()`
3. Ensure the same config is also exposed in `testsupport/testcontainers/` using the `UPPERCASE_UNDERSCORE` env var format

## Structure

```
src/main/kotlin/.../testsupport/common/
└── S3MockStarter.kt    # Abstract base: start/stop lifecycle + BaseBuilder
```
