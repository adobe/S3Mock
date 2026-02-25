# Agent Context for S3Mock

Lightweight S3 API mock server for local integration testing.

> **AGENTS.md Convention**: Module-level `AGENTS.md` files inherit from this root file and contain
> **only module-specific additions** — never duplicate rules already stated here.
> Keep all AGENTS.md files concise: no redundant sections, no generic troubleshooting,
> no restating of rules from the root.

## Tech Stack
- **Kotlin 2.3** (target JVM 17; build/compile requires JDK 25), Spring Boot 4.0.x, Maven 3.9+
- **Testing**: JUnit 5, Mockito, AssertJ, Testcontainers
- **Container**: Docker/Alpine

## Structure
```
server/              # Core implementation (Controller→Service→Store)
integration-tests/   # AWS SDK integration tests
testsupport/         # JUnit 5, Testcontainers, TestNG integrations
build-config/        # Shared build configuration
docker/              # Docker image build
```

## Architecture

**Layered**: Controller (REST) → Service (logic) → Store (filesystem)

**Key packages**: `controller/`, `service/`, `store/`, `dto/`

## DO / DON'T

> For Kotlin idioms and naming conventions, see **[docs/KOTLIN.md](docs/KOTLIN.md)**.
> For Spring Boot patterns and testing setup, see **[docs/SPRING.md](docs/SPRING.md)**.
> For testing conventions and commands, see **[docs/TESTING.md](docs/TESTING.md)**.

### DO
- Use **data classes** for DTOs with Jackson XML annotations
- Use **AWS SDK v2** for all new integration tests
- Use **JUnit 5** for all new tests
- **Update the copyright year** in the file's license header to the current year whenever you modify an existing file
- Validate XML serialization against [AWS S3 API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)

### DON'T
- DON'T use AWS SDK v1 — it has been removed in 5.x
- DON'T use JUnit 4 — it has been removed in 5.x
- DON'T declare dependency versions in sub-module POMs — all versions are managed in root `pom.xml`
- DON'T update copyright years in files you haven't modified — copyright is only bumped when a file is actually changed

## Code Style

See **[docs/KOTLIN.md](docs/KOTLIN.md)** for Kotlin idioms, naming conventions, common anti-patterns, and KDoc guidelines.

See **[docs/JAVA.md](docs/JAVA.md)** for Java idioms, naming conventions, common anti-patterns, and Javadoc guidelines.

See **[docs/SPRING.md](docs/SPRING.md)** for Spring Boot patterns, bean registration, dependency injection, controller guidelines, configuration properties, exception handling, and testing.

## XML Serialization

Jackson XML with AWS-compatible structure. Key annotations:
- `@JacksonXmlRootElement(localName = "...")`
- `@JacksonXmlProperty(localName = "...")`
- `@JacksonXmlElementWrapper(useWrapping = false)` for collections

**Important**: XML element and attribute names must match the AWS S3 API specification exactly.
Verify against [AWS API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
and existing integration tests. A compile-check is not sufficient — always verify that serialized
XML output matches the expected AWS response format by running integration tests.

## Storage

Filesystem layout:
```
<root>/<bucket>/bucketMetadata.json
<root>/<bucket>/<uuid>/binaryData + objectMetadata.json
<root>/<bucket>/<uuid>/<version-id>-binaryData  # versioning
<root>/<bucket>/multiparts/<upload-id>/<part>.part
```

## Configuration

Environment variables (prefix: `COM_ADOBE_TESTING_S3MOCK_STORE_`):
- `ROOT` - storage directory
- `RETAIN_FILES_ON_EXIT` - keep files after shutdown
- `REGION` - AWS region (default: us-east-1)
- `INITIAL_BUCKETS` - comma-separated bucket names
- `VALID_KMS_KEYS` - valid KMS ARNs

## Error Handling

Services throw `S3Exception` constants (`NO_SUCH_BUCKET`, `NO_SUCH_KEY`, `INVALID_BUCKET_NAME`, etc.).
Spring exception handlers convert them to XML `ErrorResponse` with the correct HTTP status.
See **[docs/SPRING.md](docs/SPRING.md)** for exception handling patterns and `server/AGENTS.md` for the concrete handler classes.

## Testing

See **[docs/TESTING.md](docs/TESTING.md)** for the full testing strategy, base classes, patterns, and commands.

## Build

```bash
./mvnw clean install              # Full build
./mvnw clean install -DskipDocker # Skip Docker
./mvnw verify -pl integration-tests
./mvnw ktlint:format
```

## CI/CD Pipeline

All PRs and pushes are validated by the `maven-ci-and-prb.yml` GitHub Actions workflow.

**Required gates** (all must pass before merge):
1. Compilation and build (`./mvnw clean install`)
2. Unit tests (`*Test.kt` in each module)
3. Integration tests (`*IT.kt` against Docker container)
4. ktlint (Kotlin code style)
5. Checkstyle (Java/XML code style, config in `etc/checkstyle.xml`)
6. Docker image build (unless `-DskipDocker`)

**Additional workflows**: CodeQL (security scanning), SBOM (dependency tracking), OpenSSF Scorecard, Dependabot (automated dependency updates), Stale issue management.

## Dependency Management

- **All versions** are declared in the root `pom.xml` `<properties>` section
- Sub-modules inherit versions — never declare versions in sub-module POMs
- **BOMs** are preferred for multi-artifact dependencies (Kotlin BOM, Spring Boot BOM, AWS SDK BOM)
- Prefer Kotlin stdlib / JDK APIs over adding new third-party libraries
- Dependabot manages automated version updates for Maven, Docker, and GitHub Actions

## PR & Commit Conventions

- PRs should target `main` (active development) or version maintenance branches
- Reference related GitHub issues in PR description
- Update `CHANGELOG.md` under the current version section for user-facing changes
- Ensure all CI gates pass before requesting review
- See [PR template](.github/PULL_REQUEST_TEMPLATE.md) and [Contributing Guide](.github/CONTRIBUTING.md)

## Version & Branch Strategy

- **`main`** — active development for the current major version (5.x)
- **Version branches** (`s3mock-v2`, `s3mock-v3`, `s3mock-v4`) — maintenance for previous major versions
- **Tags** follow semver: `5.0.0`, `4.11.0`, etc.
- **6.x** is planned after Spring Boot 5.x — will remove JUnit/TestNG modules and target JDK 25 LTS bytecode

## Constraints

- Path-style URLs only (not `bucket.localhost`)
- Presigned URLs accepted but not validated
- Self-signed SSL certificate
- KMS validation only, no encryption
- Not for production
