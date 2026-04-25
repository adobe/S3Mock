---
name: implement
description: Implement features, fix bugs, and modify production source code in S3Mock. Use when asked to add functionality, change behavior, or modify source files in server/, integration-tests/, or testsupport/. Not for documentation-only or test-only changes. Orchestrates lint, test, and document skills as part of the complete workflow.
---

# Implementation Skill

## Entry Criteria

Use this skill when asked to:
- Add a new S3 API operation or sub-feature
- Fix a bug in production code
- Modify existing source files in `server/`, `integration-tests/`, or `testsupport/`

Not for test-only or documentation-only changes — use the `test` or `document` skill instead.

## Before Starting

1. Read `AGENTS.md` (root) and the relevant module `AGENTS.md` (`server/AGENTS.md`, `integration-tests/AGENTS.md`, or `testsupport/AGENTS.md`).
2. Identify the AWS S3 API operation — verify element names and behavior against the [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
3. Review existing similar implementations for patterns to follow.
4. If nearby code would benefit from cleanup before adding new code, invoke the **`refactor` skill** first.

## Execution Steps

### New S3 Operations: DTO → Store → Service → Controller

Follow this order exactly — do not skip layers or add logic in the wrong layer:

1. **DTO** (`server/.../dto/`): Create data classes with Jackson XML annotations. Verify element and attribute names match the AWS S3 API specification exactly — see `INVARIANTS.md`. A compile check is not sufficient; run integration tests to verify serialized output.
2. **Store** (`server/.../store/`): Add filesystem operations. Follow existing patterns for binary data and metadata JSON.
3. **Service** (`server/.../service/`): Add business logic and validation. Throw `S3Exception` constants only — see `INVARIANTS.md`.
4. **Controller** (`server/.../controller/`): Add HTTP mapping only. Delegate all logic to services. Controllers never catch exceptions — see `INVARIANTS.md`.

### All Changes

- Update the copyright year to `2017-<current year>` in the license header of every file you modify — see `INVARIANTS.md`.
- Invoke the **`lint` skill** after implementation to fix formatting and verify style gates.
- Invoke the **`test` skill** to add or update unit and integration tests.
- Invoke the **`document` skill** to update `CHANGELOG.md`, `README.md`, and `AGENTS.md` where applicable.

## Completion Criteria

- [ ] Code compiles (`make skip-docker`)
- [ ] Unit tests pass (`make test`)
- [ ] Integration tests pass (`make integration-tests`)
- [ ] Lint gates pass (via `lint` skill)
- [ ] Copyright year updated in every modified file
- [ ] Tests added or updated (via `test` skill)
- [ ] Docs updated (via `document` skill)

## Troubleshooting

- **Build fails**: Check JDK 25 is active; run `make format` and fix remaining lint errors.
- **Tests fail on XML**: Verify element names match the AWS S3 API exactly — a compile check is insufficient. Run integration tests to confirm serialized output.
- **Docker fails**: Use `make skip-docker` to isolate the build from Docker issues.

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — architecture, DO/DON'T, build commands
- [`INVARIANTS.md`](../../../INVARIANTS.md) — hard constraints (copyright, SDK version, XML naming, layering rules)
- Relevant module `AGENTS.md` (`server/AGENTS.md`, `integration-tests/AGENTS.md`, or `testsupport/AGENTS.md`)
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) — Kotlin idioms and naming conventions
- [`docs/SPRING.md`](../../../docs/SPRING.md) — Spring Boot patterns, DI, exception handling
