---
name: document
description: Generate and update S3Mock project documentation. Use when asked to document code, update CHANGELOG.md, update README.md, update AGENTS.md files, or fix formatting, wording, or style in any existing documentation file. Also invoked at the end of the implement skill workflow.
---

# Documentation Skill

## Entry Criteria

Use this skill when asked to:
- Update `CHANGELOG.md` for a new feature or bug fix
- Update `README.md` (operations table, configuration, usage examples)
- Update `AGENTS.md` (root or module) for architectural or convention changes
- Update files in `docs/` (`KOTLIN.md`, `SPRING.md`, `TESTING.md`, `JAVA.md`)
- Fix formatting, wording, or style in any documentation file

## Documentation Files

| File | Audience | Purpose |
|---|---|---|
| `README.md` | End users | Usage, configuration, S3 operations table |
| `CHANGELOG.md` | End users | Version history, breaking changes |
| `docs/TESTING.md` | Contributors / agents | Testing strategy, base classes, patterns |
| `docs/KOTLIN.md` | Contributors / agents | Kotlin idioms, naming, anti-patterns, KDoc |
| `docs/SPRING.md` | Contributors / agents | Spring Boot patterns, DI, exception handling |
| `docs/JAVA.md` | Contributors / agents | Java idioms, naming, Javadoc |
| `AGENTS.md` (root + modules) | Agents | Architecture, conventions, guardrails |
| `.github/CONTRIBUTING.md` | Contributors | Dev setup, CLA, code reviews |

## Per-Scenario Updates

**New S3 operation**: Update the operations table in `README.md` (`:x:` → `:white_check_mark:`), add entry to `CHANGELOG.md`, update `server/AGENTS.md` if new patterns were introduced.

**Configuration change**: Update configuration table in `README.md`, update the Configuration section in `AGENTS.md`, add entry to `CHANGELOG.md`.

**Architecture change**: Update the relevant module's `AGENTS.md`, update root `AGENTS.md` if cross-cutting, add entry to `CHANGELOG.md`.

**Spring Boot pattern change**: Update `docs/SPRING.md` and `server/AGENTS.md` if it affects how controllers, services, or stores are structured.

**Kotlin/Java style change**: Update `docs/KOTLIN.md` or `docs/JAVA.md`; update root `AGENTS.md` DO/DON'T if a new guardrail is introduced.

## CHANGELOG Format

Group changes under the current version heading. If the heading doesn't exist yet, add it under `# CURRENT - 5.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT`. Follow the existing bullet structure:

```
* Features and fixes
* Refactorings
* Version updates (deliverable dependencies)
* Version updates (build dependencies)
```

Use clear, user-facing language. Note breaking changes explicitly. Reference GitHub issues/PRs where relevant.

## Style

- Concise, active voice
- Include runnable examples (Kotlin for API, shell for CLI)
- Link to [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) where applicable
- Match surrounding formatting exactly

## Execution Steps

1. Read `AGENTS.md` (root + relevant module).
2. Determine what changed and which files need updating (use the per-scenario table above).
3. Update files following the style and format rules above.
4. Verify technical accuracy against source code.
5. Verify all links and code examples are valid and runnable.

## Completion Criteria

- [ ] All appropriate files updated for the type of change
- [ ] `CHANGELOG.md` updated if the change is user-facing
- [ ] `README.md` operations or configuration table updated if applicable
- [ ] `AGENTS.md` updated if architecture or conventions changed
- [ ] Technical accuracy verified against source code
- [ ] Links and code examples are valid
- [ ] Matches surrounding style and formatting

## Resources

- [`AGENTS.md`](../../../AGENTS.md) — authoritative source for architecture and conventions
- Relevant module `AGENTS.md`
- [`CHANGELOG.md`](../../../CHANGELOG.md) — existing entries to match format
- [`README.md`](../../../README.md) — operations table and configuration table to update
