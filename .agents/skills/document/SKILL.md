---
name: document
description: Generate and update S3Mock project documentation. Use when asked to document code, update CHANGELOG.md, update README.md, update AGENTS.md files, or fix formatting, wording, or style in any existing documentation file. Also invoked at the end of the implement skill workflow.
---

# Documentation Skill

## Entry Criteria

Use when asked to update `CHANGELOG.md`, `README.md`, `AGENTS.md` (root or module), or `docs/*.md`, or to fix formatting/wording in any documentation file.

## Execution Steps

1. Read root `AGENTS.md` and the relevant module `AGENTS.md`.
2. Determine which file(s) to update by scenario:

   | Scenario | Update |
   |---|---|
   | New S3 operation | `README.md` operations table, `CHANGELOG.md`, `server/AGENTS.md` if new patterns were introduced |
   | New API surface / port | `README.md` (enablement, ports, config, ops table, Docker examples), `CHANGELOG.md`, `server/AGENTS.md` |
   | Configuration change | `README.md` configuration table, root `AGENTS.md` Configuration section, `CHANGELOG.md` |
   | Architecture change | Relevant module `AGENTS.md`, root `AGENTS.md` if cross-cutting, `CHANGELOG.md` |
   | Spring Boot pattern change | `docs/SPRING.md`, `server/AGENTS.md` |
   | Kotlin/Java style change | `docs/KOTLIN.md` / `docs/JAVA.md`, root `AGENTS.md` DO/DON'T if a new guardrail is introduced |
3. For `CHANGELOG.md`: add under the current version heading (create it under `# CURRENT - 5.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT` if missing), matching the existing `Features and fixes` / `Refactorings` / `Version updates` bullet structure. Use clear, user-facing language; note breaking changes explicitly; reference GitHub issues/PRs where relevant.
4. Match surrounding style exactly (concise, active voice, runnable examples, links to [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) where relevant).
5. Verify technical accuracy against source code, and that all links and code examples are valid and runnable.

## Completion Criteria

- [ ] All files for the scenario updated (see table above)
- [ ] Technical accuracy verified against source code
- [ ] Links and code examples are valid
- [ ] Matches surrounding style and formatting

## Resources

- [`AGENTS.md`](../../../AGENTS.md) and relevant module `AGENTS.md`
- [`CHANGELOG.md`](../../../CHANGELOG.md), [`README.md`](../../../README.md) — existing entries/tables to match
