---
name: implement
description: Implement features, fix bugs, and modify production source code in S3Mock. Use when asked to add functionality, change behavior, or modify source files in server/, integration-tests/, or testsupport/. Not for documentation-only or test-only changes. Orchestrates lint, test, and document skills as part of the complete workflow.
---

# Implementation Skill

## Entry Criteria

Use for new functionality, bug fixes, or behavior changes in `server/`, `integration-tests/`, or `testsupport/`. Not for test-only or documentation-only changes — use the `test` or `document` skill instead.

## Before Starting

1. Read root `AGENTS.md` and the relevant module `AGENTS.md` (`server/AGENTS.md`, `integration-tests/AGENTS.md`, or `testsupport/AGENTS.md`).
2. For a new S3 operation, verify element names and behavior against the [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
3. Review existing similar implementations for patterns to follow.
4. If nearby code would benefit from cleanup before adding new code, invoke the **`refactor` skill** first.

## Execution Steps

1. For a new S3 operation, follow **[server/AGENTS.md § Implementation Flow](../../../server/AGENTS.md)** (DTO → Store → Service → Controller → IT) — do not skip layers or add logic in the wrong layer.
2. Update the copyright year in every modified file (`INVARIANTS.md`).
3. Invoke the **`lint` skill**.
4. Invoke the **`test` skill** to add or update unit and integration tests.
5. Invoke the **`document` skill** to update `CHANGELOG.md`, `README.md`, and `AGENTS.md` where applicable.

## Completion Criteria

- [ ] Code compiles (`make skip-docker`)
- [ ] Unit tests pass (`make test`); integration tests pass (`make integration-tests`)
- [ ] Lint gates pass (via `lint` skill)
- [ ] Copyright year updated in every modified file
- [ ] Tests added/updated (via `test` skill); docs updated (via `document` skill)

## Troubleshooting

- **Build fails**: confirm JDK 25 is active; run `make format` and fix remaining lint errors.
- **XML test failures**: a compile check is not sufficient (`INVARIANTS.md`) — run integration tests to confirm serialized output.
- **Docker fails**: use `make skip-docker` to isolate the build from Docker issues.

## Resources

- [`AGENTS.md`](../../../AGENTS.md), [`INVARIANTS.md`](../../../INVARIANTS.md)
- Relevant module `AGENTS.md` (`server/AGENTS.md`, `integration-tests/AGENTS.md`, or `testsupport/AGENTS.md`)
