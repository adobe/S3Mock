# Architectural Decisions — S3Mock

Significant design decisions with context and rationale. Read when you need to understand *why* the project is the way it is.

---

## ADR-001: Filesystem storage over in-memory storage

**Status**: Accepted

**Context**: S3Mock needs to persist object data and metadata between requests. In-memory storage is simpler but cannot support large object bodies, versioning file trees, or persistence across container restarts.

**Decision**: Store all object data as binary files and all metadata as JSON sidecars on the local filesystem under a configurable root directory.

**Consequences**:
- Object data is streamed from disk, never fully buffered in memory — supports arbitrarily large objects
- Metadata is re-read from disk on every request — no stale-cache bugs, but no read cache either
- Versioned objects store separate `<version-id>-binaryData` and `<version-id>-objectMetadata.json` files alongside the current version
- The root directory is deleted on JVM shutdown by default (`StoreCleaner`); opt out with `retainFilesOnExit=true`
- 5.x filesystem layout is **incompatible** with 4.x — Jackson 3 migration changed the serialized metadata format

---

## ADR-002: Spring Boot as HTTP server framework

**Status**: Accepted

**Context**: S3Mock needs an HTTP server that supports dual connectors (HTTP + HTTPS on separate ports), customizable Tomcat configuration (encoded slash handling), and a rich testing ecosystem.

**Decision**: Use Spring Boot (currently 4.0.x) with an embedded Tomcat server.

**Consequences**:
- Spring Boot dependency management simplifies version alignment across Jackson, Tomcat, and test libraries
- Dual-connector setup requires a custom `TomcatServletWebServerFactory` bean in `S3MockConfiguration`
- Encoded slash (`%2F`) and backslash handling require explicit `EncodedSolidusHandling.DECODE` configuration
- In-process test integrations (JUnit 5 extension, TestNG listener) pull Spring Boot transitively — callers must be compatible with the Spring Boot version used by S3Mock
- 4.x → Spring Boot 4.x + Spring Framework 7.x is a breaking transitive dependency change for in-process callers

---

## ADR-003: Kotlin as primary language

**Status**: Accepted

**Context**: S3Mock was originally written in Java. Kotlin was introduced to reduce boilerplate for data classes, null safety, and idiomatic collection operations.

**Decision**: All new code is written in Kotlin. Legacy Java code is migrated opportunistically. Kotlin version tracks the latest stable release; API/language compatibility targets 2.2; JVM target is 17 (bytecode); build toolchain uses JDK 25.

**Consequences**:
- Data classes replace verbose Java POJOs for DTOs and store metadata objects
- `data class` `copy()` is used for immutable updates (e.g., creating delete markers)
- Null safety enforced at the type system level — services use `?` types where S3 allows absent fields
- Kotlin 2.x compiles against JDK 25 toolchain but emits JDK 17 bytecode — compatible with JDK 17+ runtimes
- See **[docs/KOTLIN.md](KOTLIN.md)** for idioms, naming conventions, and anti-patterns

---

## ADR-004: AWS SDK v2 only (v1 removed in 5.x)

**Status**: Accepted (v1 removed in 5.0.0)

**Context**: AWS deprecated SDK for Java v1 in late 2024. S3Mock integration tests historically used both v1 and v2 clients. Maintaining two client versions added test complexity with no benefit.

**Decision**: AWS SDK v1 is removed entirely in 5.x. All integration tests use AWS SDK v2 (`software.amazon.awssdk`). The AWS Kotlin SDK (`aws.smithy.kotlin`) is also available in integration tests.

**Consequences**:
- All new `*IT.kt` integration tests must use `software.amazon.awssdk` or `aws.smithy.kotlin`
- Any v1 (`com.amazonaws`) import is a **Must fix** violation
- SDK v2 `S3Client` / `S3AsyncClient` are configured in `S3TestBase` — extend it rather than creating your own client

---

## ADR-005: Testcontainers as recommended deployment approach

**Status**: Accepted

**Context**: S3Mock can run in-process (JUnit 5 extension, TestNG listener) or in Docker (Testcontainers). In-process avoids Docker but creates classpath coupling and Spring context conflicts with the caller's test context.

**Decision**: Testcontainers (`S3MockContainer`) is the recommended approach. In-process JUnit 5 and TestNG integrations are deprecated in 5.x and will be removed in 6.x.

**Consequences**:
- `testsupport/testcontainers` is the primary integration module; `testsupport/common` and `testsupport/junit5` / `testsupport/testng` are legacy
- Docker must be available in CI environments
- Testcontainers handles port mapping, container lifecycle, and health checks automatically
- See **[testsupport/testcontainers/AGENTS.md](../testsupport/testcontainers/AGENTS.md)** for known footguns

---

## ADR-006: Path-style URLs only

**Status**: Accepted

**Context**: AWS S3 supports both path-style (`http://host/bucket/key`) and virtual-hosted-style (`http://bucket.host/key`) URLs. Virtual-hosted-style requires DNS wildcard configuration or local `/etc/hosts` manipulation.

**Decision**: S3Mock supports path-style URLs only. Virtual-hosted-style is not implemented and not planned.

**Consequences**:
- AWS SDK clients must be configured with `forcePathStyle(true)` (v2) or equivalent
- `S3TestBase` and `S3MockContainer` configure this automatically for users
- Any request using virtual-hosted-style will fail with 404 or be misrouted

---

## ADR-007: Jackson 3 (`tools.jackson`) replacing Jackson 2

**Status**: Accepted (migrated in 5.0.0 with Spring Boot 4.x)

**Context**: Spring Boot 4.x migrated from Jackson 2 (`com.fasterxml.jackson`) to Jackson 3 (`tools.jackson`). All XML and JSON serialization must use the new package names.

**Decision**: All Jackson annotations use `tools.jackson.*` packages. Legacy `com.fasterxml.jackson.dataformat.xml.annotation` annotations (`@JacksonXmlRootElement`, `@JacksonXmlProperty`) are banned.

**Consequences**:
- `@JsonRootName("...", namespace = "...")` replaces `@JacksonXmlRootElement`
- `@JsonProperty("...", namespace = "...")` replaces `@JacksonXmlProperty`
- `@JacksonXmlElementWrapper` from `tools.jackson.dataformat.xml.annotation` is still used for collections
- Filesystem metadata written by 4.x (`bucketMetadata.json`, `objectMetadata.json`) is not readable by 5.x — layout is incompatible
- Any `com.fasterxml.jackson` import in a DTO or store class is a **Must fix** violation

---

## ADR-008: No authentication or authorization by design

**Status**: Accepted (permanent)

**Context**: S3Mock is a local testing tool. Adding real authentication would require clients to generate valid AWS Signature V4 tokens, which adds setup complexity with no benefit in a local test context.

**Decision**: S3Mock accepts all requests regardless of credentials. AWS Signature V4 headers are parsed for compatibility but never validated. Presigned URLs are accepted but signatures are not checked.

**Consequences**:
- Any client can read or write any bucket or object — never deploy S3Mock in a shared or internet-accessible environment
- KMS key ARN format is validated (to catch configuration errors) but no encryption is performed
- The SSL certificate is self-signed — clients must disable certificate validation
- Adding signature validation is explicitly prohibited — see **[INVARIANTS.md](../INVARIANTS.md)**
