---
name: document
description: Generate or update project documentation. Use when asked to document code, create docs, explain features, or fix formatting, wording, or style in existing documentation files.
---

# Documentation Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before making changes.

## Documentation Files

| File | Audience | Purpose |
|------|----------|---------|
| `README.md` | End users | Usage, configuration, operations table |
| `CHANGELOG.md` | End users | Version history, breaking changes |
| `docs/TESTING.md` | Contributors / AI agents | Testing strategy, base classes, patterns, commands |
| `docs/KOTLIN.md` | Contributors / AI agents | Kotlin idioms, naming conventions, anti-patterns, KDoc |
| `docs/SPRING.md` | Contributors / AI agents | Spring Boot patterns, DI, controllers, exception handling |
| `docs/JAVA.md` | Contributors / AI agents | Java idioms, naming conventions, anti-patterns, Javadoc |
| `AGENTS.md` (root + modules) | AI agents | Architecture, conventions, guardrails |
| `.github/CONTRIBUTING.md` | Contributors | Dev setup, code reviews |

## What to Update

**New S3 operation**: operations table in `README.md` (`:x:` → `:white_check_mark:`), `CHANGELOG.md`, `server/AGENTS.md` if new patterns introduced.

**Configuration change**: configuration table in `README.md`, configuration section in `AGENTS.md`, `CHANGELOG.md`.

**Architecture change**: relevant module's `AGENTS.md`, root `AGENTS.md` if cross-cutting, `CHANGELOG.md`.

**Spring Boot pattern change**: `docs/SPRING.md`, `server/AGENTS.md` if the change affects how controllers/services/stores are structured.

**Kotlin/Java style change**: `docs/KOTLIN.md` or `docs/JAVA.md`, root `AGENTS.md` DO/DON'T section if it introduces a new guardrail.

## CHANGELOG Format

Group changes under the current version heading (e.g., `## 5.0.1`). If that heading doesn't exist yet, add it under `# CURRENT - 5.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT`. Use clear user-facing language, note breaking changes explicitly, and reference GitHub issues/PRs where relevant. Follow the existing bullet structure: `* Features and fixes`, `* Refactorings`, `* Version updates (deliverable dependencies)`, `* Version updates (build dependencies)`.

## Style

Concise, active voice. Include runnable examples (Kotlin for API, shell for CLI). Link to AWS S3 API docs. Match existing formatting.

## Checklist

- [ ] Verify technical accuracy against source code
- [ ] Ensure links and code examples are valid
- [ ] Match surrounding style and formatting
