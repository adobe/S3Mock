# Agent Context for S3Mock

Lightweight S3 API mock server for local integration testing.

> **Read [INVARIANTS.md](INVARIANTS.md) first** — it lists non-negotiable constraints that apply to all work in this repository.

> **AGENTS.md Convention**: Module-level `AGENTS.md` files inherit from this root file and contain
> **only module-specific additions** — never duplicate rules already stated here.
> Keep all AGENTS.md files concise: no redundant sections, no generic troubleshooting,
> no restating of rules from the root.

## Tech Stack
- **Kotlin 2.3+** (language/API compatibility: 2.2; JVM target: 17; build toolchain: JDK 25 — per Spring Boot 4.x guidance), Spring Boot 4.0.x, Maven 3.9+
- **Testing**: JUnit 5, Mockito, AssertJ, Testcontainers
- **Container**: Docker/Alpine

## Structure

| Module | Description                                                                                                                       | Agent context |
|---|-----------------------------------------------------------------------------------------------------------------------------------|---|
| `server/` | Core implementation (Controller→Service→Store)                                                                                    | [server/AGENTS.md](server/AGENTS.md) |
| `integration-tests/` | AWS SDK integration tests                                                                                                         | [integration-tests/AGENTS.md](integration-tests/AGENTS.md) |
| `testsupport/` | JUnit 5, Testcontainers, TestNG integrations                                                                                      | [testsupport/AGENTS.md](testsupport/AGENTS.md) |
| `docker/` | Docker image build                                                                                                                | — |
| `docs/` | Convention docs ([KOTLIN.md](docs/KOTLIN.md), [SPRING.md](docs/SPRING.md), [TESTING.md](docs/TESTING.md), [JAVA.md](docs/JAVA.md) | — |

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
- Validate XML serialization against [AWS S3 API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)

### DON'T

See **[INVARIANTS.md](INVARIANTS.md)** for all non-negotiable constraints — SDK version, test framework, XML naming, layering rules, copyright, and runtime scope.

## Code Style

See **[docs/KOTLIN.md](docs/KOTLIN.md)** for Kotlin idioms, naming conventions, common anti-patterns, and KDoc guidelines.

See **[docs/JAVA.md](docs/JAVA.md)** for Java idioms, naming conventions, common anti-patterns, and Javadoc guidelines.

See **[docs/SPRING.md](docs/SPRING.md)** for Spring Boot patterns, bean registration, dependency injection, controller guidelines, configuration properties, exception handling, and testing.

## XML Serialization

Jackson 3 XML with AWS-compatible structure. Key annotations (Jackson 3 — `tools.jackson` packages):
- `@JsonRootName("...", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")` — replaces old `@JacksonXmlRootElement`
- `@JsonProperty("...", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")` — replaces old `@JacksonXmlProperty`
- `@JacksonXmlElementWrapper(useWrapping = false)` for collections — from `tools.jackson.dataformat.xml.annotation`

See `dto/ListBucketResult.kt` for a representative example. XML names must match the AWS S3 API exactly — see **[INVARIANTS.md](INVARIANTS.md)**.

## Storage

Filesystem layout:
```
<root>/<bucket>/bucketMetadata.json
<root>/<bucket>/<uuid>/binaryData
<root>/<bucket>/<uuid>/objectMetadata.json
<root>/<bucket>/<uuid>/<version-id>-binaryData              # versioning
<root>/<bucket>/<uuid>/<version-id>-objectMetadata.json      # versioning
<root>/<bucket>/multiparts/<upload-id>/multipartMetadata.json
<root>/<bucket>/multiparts/<upload-id>/<part>.part
```

## Configuration

Environment variables:
- `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT` - storage directory
- `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT` - keep files after shutdown (default: false)
- `COM_ADOBE_TESTING_S3MOCK_STORE_REGION` - AWS region (default: us-east-1)
- `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS` - comma-separated bucket names
- `COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS` - valid KMS ARNs
- `COM_ADOBE_TESTING_S3MOCK_CONTROLLER_CONTEXT_PATH` - base context path for all endpoints (default: "")

Spring profiles (activate via `SPRING_PROFILES_ACTIVE`):
- `debug` - debug logging + activates `actuator` profile
- `trace` - trace logging + activates `actuator` profile
- `actuator` - enables JMX and all Spring Boot Actuator endpoints

Actuator endpoints are **disabled by default** (`management.endpoints.access.default=none`).
Enable via `SPRING_PROFILES_ACTIVE=actuator` (or `debug`/`trace`) or by setting
`MANAGEMENT_ENDPOINTS_ACCESS_DEFAULT=unrestricted` directly.

Health check endpoints:
- `/favicon.ico` — always available, returns `200 OK` (used by Testcontainers and integration tests)
- `/actuator/health` — only available when `actuator` profile is active

## Error Handling

Services throw `S3Exception` constants (`NO_SUCH_BUCKET`, `NO_SUCH_KEY`, `INVALID_BUCKET_NAME`, etc.).
Spring exception handlers convert them to XML `ErrorResponse` with the correct HTTP status.
See **[docs/SPRING.md](docs/SPRING.md)** for exception handling patterns and `server/AGENTS.md` for the concrete handler classes.

## Testing

See **[docs/TESTING.md](docs/TESTING.md)** for the full testing strategy, base classes, patterns, and commands.

## Build

```bash
make install              # Full build
make skip-docker          # Skip Docker
make test                 # Unit tests only
make integration-tests    # Run integration tests
make format               # Format Kotlin code (ktlint)
make run                  # Run S3Mock from source (Spring Boot)
make sort                 # Sort POM files (sortpom)
```

Use the **`lint` skill** to fix formatting and verify style gates (ktlint + Checkstyle) pass.

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
- Sub-modules inherit versions — never declare versions in sub-module POMs (see [INVARIANTS.md](INVARIANTS.md))
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
- **Version branches** (`s3mock-v4`) — maintenance for previous major version; `s3mock-v2` and `s3mock-v3` are EOL
- **Tags** follow semver: `5.0.0`, `4.11.0`, etc.
- **6.x** is planned after Spring Boot 5.x — will remove JUnit/TestNG modules and target JDK 25 LTS bytecode
