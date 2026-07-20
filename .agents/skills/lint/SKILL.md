---
name: lint
description: Fix code style issues and ensure linting CI gates pass in S3Mock. Use when ktlint or Checkstyle violations are reported, when asked to fix formatting, or as a required final step after any code change. Runs ktlint for Kotlin source files and Checkstyle for Java and XML files.
---

# Lint Skill

## Entry Criteria

Use when ktlint/Checkstyle violations are reported, when asked to fix formatting, or when invoked as a final step by the `implement`, `test`, or `refactor` skill.

## Tools

| Tool | Target | Config | Auto-fix |
|---|---|---|---|
| ktlint | Kotlin source files | `.editorconfig` | Yes — `make format` |
| Checkstyle | Java source + XML files | `etc/checkstyle.xml` | No — fix manually per `docs/JAVA.md` |

## Execution Steps

1. `make format` — auto-fixes Kotlin style issues.
2. `make lint` — reports remaining ktlint and Checkstyle violations.
3. `make sort` — run if any `pom.xml` was added or modified during this task.
4. Fix remaining Checkstyle (Java/XML) violations manually, per `docs/JAVA.md` and `docs/KOTLIN.md`.
5. `make install` — confirm all linting CI gates pass before finishing.

## Completion Criteria

- [ ] `make format` run
- [ ] `make lint` passes with no violations
- [ ] `make sort` run if any `pom.xml` was modified
- [ ] `make install` passes all CI gates

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — build command reference
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md), [`docs/JAVA.md`](../../../docs/JAVA.md) — style conventions
- [`etc/checkstyle.xml`](../../../etc/checkstyle.xml), [`.editorconfig`](../../../.editorconfig)
