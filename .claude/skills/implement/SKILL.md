---
name: implement
description: Implement features, fix bugs, or refactor code. Use when asked to add functionality, modify code, or improve structure.
---

# Implementation Skill — S3Mock

> **Before making any changes**: Read `AGENTS.md` (root + relevant module). These files are the authoritative source for architecture, conventions, and guardrails — they must be read, not skipped.

## Skill Workflow

A complete feature implementation requires these skills — run them in sequence:

1. **refactor** skill — if nearby existing code, tests, or configuration would benefit from cleanup *before* adding new code, do it first rather than working around it
2. **implement** (this skill) — write the production code
3. **test** skill — add/update unit and integration tests (`*Test.kt`, `*IT.kt`)
4. **document** skill — update `CHANGELOG.md`, `README.md`, and `AGENTS.md` where applicable

> **Prefer refactoring over workarounds**: if you find yourself adding complexity to work around existing code, stop and invoke the `refactor` skill to clean it up first.

## Implementation Flow for New S3 Operations

Follow **DTO → Store → Service → Controller** (see AGENTS.md Architecture):

1. **DTO** (`server/.../dto/`): Data classes with Jackson XML annotations. Verify element names against [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
2. **Store** (`server/.../store/`): Filesystem operations. Follow existing patterns for metadata JSON and binary data.
3. **Service** (`server/.../service/`): Business logic, validation, store coordination. Throw `S3Exception` constants.
4. **Controller** (`server/.../controller/`): HTTP mapping only — delegate all logic to services.

## Checklist

- [ ] Read root + module `AGENTS.md` (required before any other step)
- [ ] Identify the S3 API operation ([AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html))
- [ ] Review existing similar implementations
- [ ] Run `./mvnw ktlint:format` then `./mvnw clean install`
- [ ] Invoke the **`test` skill** to add/update unit and integration tests
- [ ] Invoke the **`document` skill** to update `CHANGELOG.md`, `README.md`, and `AGENTS.md`

## Troubleshooting

- **Build fails**: Check Java 25, run `./mvnw ktlint:format`
- **Tests fail**: Ensure XML matches AWS API exactly — run integration tests
- **Docker fails**: Try `./mvnw clean install -DskipDocker` to isolate
