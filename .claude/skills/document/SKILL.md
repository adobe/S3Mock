---
name: document
description: Generate or update project documentation. Use when asked to document code, create docs, or explain features.
---

# Documentation Skill — S3Mock

Read `AGENTS.md` (root + relevant module) before making changes.

## Documentation Files

| File | Audience | Purpose |
|------|----------|---------|
| `README.md` | End users | Usage, configuration, operations table |
| `CHANGELOG.md` | End users | Version history, breaking changes |
| `AGENTS.md` (root + modules) | AI agents | Architecture, conventions, guardrails |
| `.github/CONTRIBUTING.md` | Contributors | Dev setup, code reviews |

## What to Update

**New S3 operation**: operations table in `README.md` (`:x:` → `:white_check_mark:`), `CHANGELOG.md`, `server/AGENTS.md` if new patterns introduced.

**Configuration change**: configuration table in `README.md`, configuration section in `AGENTS.md`, `CHANGELOG.md`.

**Architecture change**: relevant module's `AGENTS.md`, root `AGENTS.md` if cross-cutting, `CHANGELOG.md`.

## CHANGELOG Format

Follow existing pattern: group under current version heading, clear user-facing language, note breaking changes, reference issues/PRs.

## Style

Concise, active voice. Include runnable examples (Kotlin for API, shell for CLI). Link to AWS S3 API docs. Match existing formatting.

## Checklist

- Verify technical accuracy against source code
- Ensure links and code examples are valid
- Match surrounding style and formatting
