---
name: lint
description: Fix code style issues and ensure linting CI gates pass in S3Mock. Use when ktlint or Checkstyle violations are reported, when asked to fix formatting, or as a required final step after any code change. Runs ktlint for Kotlin source files and Checkstyle for Java and XML files.
---

# Lint Skill

## Entry Criteria

Use this skill when:
- ktlint or Checkstyle violations are reported
- Asked to fix formatting issues
- Invoked as a final step by the `implement`, `test`, or `refactor` skill

## Tools

| Tool | Target | Config | Auto-fix? |
|---|---|---|---|
| **ktlint** | Kotlin source files | `.editorconfig` | Yes — `make format` |
| **Checkstyle** | Java source + XML files | `etc/checkstyle.xml` | No — fix manually |

## Execution Steps

1. **`make format`** — auto-formats all Kotlin files with ktlint. Fixes the vast majority of Kotlin style issues.
2. **`./mvnw checkstyle:check`** — reports Checkstyle violations for Java and XML files. Fix violations manually.
3. **`make sort`** — run this if any `pom.xml` was added or modified during this task.
4. Fix any remaining violations manually (see resources below for style guides).
5. **`make install`** — confirm all linting CI gates pass before finishing.

## Common Violations

### ktlint (Kotlin)

`make format` auto-fixes most of these:
- Wrong indentation (2 spaces)
- Unused or wildcard imports
- Missing trailing newline
- Line too long (max 120 characters)

To check without modifying files: `./mvnw ktlint:check`

### Checkstyle (Java / XML)

Fix these manually:
- Wrong indentation (2 spaces, per `.editorconfig`)
- Line too long (max 120 characters)
- Import ordering
- Missing or malformed Javadoc

## Completion Criteria

- [ ] `make format` run (Kotlin auto-fixed)
- [ ] `./mvnw checkstyle:check` passes with no violations
- [ ] `make sort` run if any `pom.xml` was modified
- [ ] `make install` passes all CI gates

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — build command reference
- [`docs/KOTLIN.md`](../../../docs/KOTLIN.md) — Kotlin style conventions
- [`docs/JAVA.md`](../../../docs/JAVA.md) — Java style conventions
- [`etc/checkstyle.xml`](../../../etc/checkstyle.xml) — Checkstyle configuration
- [`.editorconfig`](../../../.editorconfig) — indentation and line-length settings
