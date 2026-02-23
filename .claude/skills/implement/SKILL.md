---
name: implement
description: Implement features, fix bugs, or refactor code. Use when asked to add functionality, modify code, or improve structure.
---

# Implementation Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before making changes — especially DO/DON'T, code style, and architecture sections.

## Implementation Flow for New S3 Operations

Follow **DTO → Store → Service → Controller** (see AGENTS.md Architecture):

1. **DTO** (`server/.../dto/`): Data classes with Jackson XML annotations. Verify element names against [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
2. **Store** (`server/.../store/`): Filesystem operations. Follow existing patterns for metadata JSON and binary data.
3. **Service** (`server/.../service/`): Business logic, validation, store coordination. Throw `S3Exception` constants.
4. **Controller** (`server/.../controller/`): HTTP mapping only — delegate all logic to services.

## Checklist

- [ ] Read root + module `AGENTS.md`
- [ ] Identify the S3 API operation ([AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html))
- [ ] Review existing similar implementations
- [ ] Run `./mvnw ktlint:format` then `./mvnw clean install`
- [ ] Add/update unit tests (`*Test.kt`) and integration tests (`*IT.kt`)
- [ ] Update `CHANGELOG.md` and operations table in `README.md` if applicable

## Troubleshooting

- **Build fails**: Check Java 25, run `./mvnw ktlint:format`
- **Tests fail**: Ensure XML matches AWS API exactly — run integration tests
- **Docker fails**: Try `./mvnw clean install -DskipDocker` to isolate
