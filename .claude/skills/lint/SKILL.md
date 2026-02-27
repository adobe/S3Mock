---
name: lint
description: Fix code style issues and ensure linting passes. Use when asked to fix lint errors, formatting issues, or when ktlint or Checkstyle violations are reported.
---

# Lint Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before making changes.

## Linters

S3Mock uses two linting tools that run as required CI gates:

| Tool | Target | Config | Auto-fix? |
|------|--------|--------|-----------|
| **ktlint** | Kotlin source files | `.editorconfig` | Yes — `make format` |
| **Checkstyle** | Java source + XML files | `etc/checkstyle.xml` | No — fix manually |

Both run automatically as part of the full build (`make install`).

## Workflow

1. **Run `make format`** — auto-formats all Kotlin files with ktlint. Fixes the vast majority of Kotlin style issues.
2. **Run `./mvnw checkstyle:check`** — reports Checkstyle violations for Java and XML files. Fix violations manually.
3. **Re-run `make install`** — confirm all linting gates pass before submitting.

## ktlint (Kotlin)

`make format` auto-fixes most issues. Common violations:
- Wrong indentation (2 spaces for Kotlin)
- Unused or wildcard imports
- Missing trailing newline
- Line length (ktlint default: max 120 characters)

To check without modifying files: `./mvnw ktlint:check`

## Checkstyle (Java / XML)

Violations must be fixed manually. Common violations (config in `etc/checkstyle.xml`):
- Wrong indentation (2 spaces, per `.editorconfig`)
- Line length (max 120 characters)
- Import ordering
- Missing or malformed Javadoc

See **[docs/JAVA.md](../../../docs/JAVA.md)** for Java style conventions.

## Checklist

- [ ] Read root + module `AGENTS.md` for the files being changed
- [ ] Run `make format` to auto-fix Kotlin style
- [ ] Run `./mvnw checkstyle:check` to check Java/XML style
- [ ] Fix any remaining violations manually (see `docs/JAVA.md` for Java conventions, `docs/KOTLIN.md` for Kotlin)
- [ ] Re-run `make install` to confirm all CI gates pass
