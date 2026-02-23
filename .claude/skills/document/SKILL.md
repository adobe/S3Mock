---
name: document
description: Generate or update project documentation. Use when asked to document code, create docs, or explain features.
---

# Documentation Skill — S3Mock

Generate and maintain documentation for the S3Mock project.

## When to Use

- Creating or updating README.md
- Updating the CHANGELOG.md for new features/fixes
- Updating the S3 operations support table
- Documenting new configuration options
- Updating AGENTS.md files when architecture changes

## Pre-Flight Checklist

- [ ] Read existing `AGENTS.md` for project conventions and structure
- [ ] Review the documentation being updated for current style and tone
- [ ] Identify target audience (end users for README, agents for AGENTS.md, contributors for CONTRIBUTING.md)

## S3Mock Documentation Structure

| File | Audience | Purpose |
|------|----------|---------|
| `README.md` | End users & contributors | Usage, configuration, quick start |
| `CHANGELOG.md` | End users | Version history, breaking changes, migration notes |
| `AGENTS.md` (root) | AI agents & contributors | Architecture, code style, DO/DON'T guardrails |
| `server/AGENTS.md` | AI agents | Server module implementation details |
| `integration-tests/AGENTS.md` | AI agents | Integration test patterns and helpers |
| `testsupport/AGENTS.md` | AI agents | Test framework integration details |
| `.github/CONTRIBUTING.md` | Contributors | How to contribute, CLA, code reviews |

## Documentation Tasks

### When a New S3 Operation is Implemented
1. Update the **operations table** in `README.md` — change `:x:` to `:white_check_mark:` for the operation
2. Add a CHANGELOG entry under the current version section in `CHANGELOG.md`
3. Update `server/AGENTS.md` if the implementation introduces new patterns

### When Configuration Changes
1. Update the **Configuration table** in `README.md`
2. Update the **Configuration section** in `AGENTS.md`
3. Add a CHANGELOG entry

### When Architecture Changes
1. Update the relevant module's `AGENTS.md`
2. Update the root `AGENTS.md` if the change affects the overall structure
3. Add a CHANGELOG entry for breaking changes

### CHANGELOG Format
Follow the existing pattern in `CHANGELOG.md`:
- Group changes under the current version heading (e.g., `## 5.0.0`)
- Use clear, user-facing language
- Note breaking changes prominently
- Reference related GitHub issues or PRs

## Writing Style

- **Concise**: Short sentences, active voice
- **Code examples**: Include runnable examples where possible (Kotlin for API, shell for CLI)
- **Links**: Reference AWS S3 API docs for operations, link to source files for implementations
- **Consistent**: Match existing formatting — Markdown headings, table alignment, badge style

## Post-Flight Checklist

- [ ] Technical accuracy verified against source code
- [ ] Code examples compile/run correctly
- [ ] Links are valid (internal file paths, external URLs)
- [ ] Consistent style with surrounding documentation
- [ ] Markdown renders correctly (tables, code blocks, badges)
- [ ] No outdated version numbers or deprecated references

## Output

Provide documentation ready to integrate into the appropriate project files, matching existing conventions.
