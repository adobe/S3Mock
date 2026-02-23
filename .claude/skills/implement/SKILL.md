---
name: implement
description: Implement features, fix bugs, or refactor code. Use when asked to add functionality, modify code, or improve structure.
---

# Implementation Skill — S3Mock

Implement features and fix bugs in the S3Mock project (Kotlin 2.3, Spring Boot 4.0.x, Maven).

## When to Use

- Adding new S3 API operations
- Fixing bugs in existing operations
- Refactoring server, service, or store layers
- Updating DTOs or XML serialization

## Pre-Flight Checklist

- [ ] Read the root `AGENTS.md` — especially the DO/DON'T section
- [ ] Read the module-specific `AGENTS.md` (`server/AGENTS.md`, etc.)
- [ ] Check `CHANGELOG.md` for planned changes or deprecations
- [ ] Identify which S3 API operation is being implemented (check [AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html))
- [ ] Review existing similar implementations in the codebase

## Implementation Flow for New S3 Operations

Follow the **DTO → Store → Service → Controller** layered architecture:

### 1. DTO (`server/src/main/kotlin/com/adobe/testing/s3mock/dto/`)
- Create request/response data classes with Jackson XML annotations
- Use `@JacksonXmlRootElement(localName = "...")` matching the AWS API element name exactly
- Use `@JacksonXmlProperty(localName = "...")` for properties
- Use `@JacksonXmlElementWrapper(useWrapping = false)` for collections
- Verify naming against [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)

### 2. Store (`server/src/main/kotlin/com/adobe/testing/s3mock/store/`)
- Add filesystem operations to `BucketStore` or `ObjectStore`
- Follow existing patterns for metadata JSON and binary data storage
- Handle file I/O with proper error handling

### 3. Service (`server/src/main/kotlin/com/adobe/testing/s3mock/service/`)
- Add business logic, validation, and store coordination
- Throw S3 exceptions (`NoSuchBucketException`, `NoSuchKeyException`, etc.)
- Use constructor injection for dependencies

### 4. Controller (`server/src/main/kotlin/com/adobe/testing/s3mock/controller/`)
- Add HTTP endpoint mapping (`@GetMapping`, `@PutMapping`, etc.)
- Controllers only map HTTP — delegate all logic to services
- Return proper HTTP status codes and headers (ETag, Content-Type, etc.)

## Code Standards

- **Language**: Kotlin 2.3, JVM target 17
- **DI**: Constructor injection only — never `@Autowired` or field injection
- **DTOs**: Data classes with `val` properties
- **Null safety**: Use `?`, `?.`, `?:` — avoid `!!`
- **Functions**: Expression bodies for simple functions
- **Dependencies**: Prefer Kotlin stdlib over third-party libraries
- **Versions**: All dependency versions in root `pom.xml` only

## Post-Flight Checklist

- [ ] Run `./mvnw ktlint:format` to fix code style
- [ ] Run `./mvnw clean install` to verify build
- [ ] Verify no checkstyle violations
- [ ] Add/update unit tests (`*Test.kt`) for new service/store logic
- [ ] Add/update integration tests (`*IT.kt`) for new endpoints
- [ ] Update `CHANGELOG.md` under the current version section
- [ ] Update the operations table in `README.md` if a new S3 operation was added

## Troubleshooting

- **Build fails**: Check Java version (`java -version` — needs 25), run `./mvnw ktlint:format`
- **Checkstyle fails**: Review rules in `etc/checkstyle.xml` — common issues are import ordering and missing Javadoc
- **Tests fail after changes**: Ensure XML serialization matches AWS API exactly — compare element names against [AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- **Docker build fails**: Try `./mvnw clean install -DskipDocker` first to isolate the issue

## Output

Provide clean, well-structured Kotlin code following the layered architecture and project conventions defined in AGENTS.md.
