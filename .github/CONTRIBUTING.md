# Contributing

Thanks for choosing to contribute!

> **Before making any changes**, read [INVARIANTS.md](../INVARIANTS.md) — it lists non-negotiable constraints.

The following are a set of guidelines to follow when contributing to this project.

## Code Of Conduct

This project adheres to the Adobe [code of conduct](../CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [Grp-opensourceoffice@adobe.com](mailto:Grp-opensourceoffice@adobe.com).

## Contributor License Agreement

All third-party contributions to this project must be accompanied by a signed contributor license. This gives Adobe permission to redistribute your contributions as part of the project. Sign our [CLA](http://adobe.github.io/cla.html). You only need to submit an Adobe CLA one time, so if you have submitted one previously, you are good to go!

## Development Setup

**Prerequisites:**
- Java 25 (compile/run; JVM target 17 with Kotlin language/api 2.2, per Spring Boot 4.x guidance)
- Maven 3.9+ (use the included `./mvnw` wrapper)
- Docker (for Docker build and integration tests)

**Build and verify:**
```shell
make install           # Full build with Docker
make skip-docker       # Skip Docker (faster, for unit tests only)
make integration-tests # Run integration tests
make format            # Format Kotlin code
```

## Adding a New S3 Operation

See the full implementation checklist in **[server/AGENTS.md § Implementation Flow](../server/AGENTS.md)** — it is the authoritative step-by-step guide (DTO → Store → Service → Controller → IT → docs).

## Code Review Focus

In priority order:
1. AWS API correctness (XML names, status codes, error codes)
2. Layer violations (logic in controllers, HTTP in services)
3. Locking correctness (all filesystem writes acquire the appropriate lock)
4. Constraint violations (SDK v1, JUnit 4, legacy Jackson annotations)
5. Test quality (mocked SDK clients in `*IT.kt`, shared state between tests)

## Architecture

S3Mock follows a **Controller - Service - Store** layered architecture. For detailed architecture documentation, code style guidelines, and project conventions, see the [AGENTS.md](../AGENTS.md) in the project root.

Module-specific documentation:
- [Server Module](../server/AGENTS.md) - core implementation
- [Integration Tests](../integration-tests/AGENTS.md) - test patterns
- [Test Support](../testsupport/AGENTS.md) - framework integrations

## Code Style

- **Kotlin**: Enforced by ktlint — run `make format` before submitting
- **XML/Java**: Enforced by Checkstyle — configuration in [`etc/checkstyle.xml`](../etc/checkstyle.xml)
- **Key conventions**: Constructor injection, data classes for DTOs, backtick test names, `val` over `var`
- See the DO / DON'T section in [AGENTS.md](../AGENTS.md) for the full list

## Code Reviews

All submissions should come in the form of pull requests and need to be reviewed by project committers. Read [GitHub's pull request documentation](https://help.github.com/articles/about-pull-requests/) for more information on sending pull requests.

## Testing

All submissions must include tests for any new functionality or bug fixes.

S3Mock uses three test levels:
1. **Unit tests** (`*Test.kt`) - Spring Boot tests with `@MockitoBean` for mocking, in `server/src/test/`
2. **Spring Boot tests** - component-level coverage with Spring context
3. **Integration tests** (`*IT.kt`) - end-to-end tests against the Docker container using real AWS SDK v2 clients, in `integration-tests/src/test/`

Ensure your code has coverage from at least one of these test types.

## Submitting Changes

1. Ensure all CI gates pass (build, tests, ktlint, checkstyle, Docker)
2. Update [CHANGELOG.md](../CHANGELOG.md) under the current version section for user-facing changes
3. Update documentation if applicable (README.md, AGENTS.md)
4. Follow the [pull request template](PULL_REQUEST_TEMPLATE.md) when submitting

## For Agents

### What you can do without approval

- Edit any file in `server/`, `integration-tests/`, `testsupport/`, `docs/`, `AGENTS.md`, `INVARIANTS.md`, `CHANGELOG.md`
- Run `make test`, `make lint`, `make fmt`, `make typecheck`, `make check`
- Create branches, stage files, draft commit messages

### What requires human approval

- `git push` or creating a PR
- Modifying `.github/` workflows
- Changing `pom.xml` dependency versions
- Deleting files (check with the human first)
- Any change that touches the Docker image, Dockerfile, or CI configuration

### Escalation checklist

Before flagging a blocker, verify:
- [ ] `make lint` passes (ktlint + Checkstyle)
- [ ] `make typecheck` passes (no compilation errors)
- [ ] `make test` passes (unit tests)
- [ ] The change you're making is within the scope requested

If `make integration-tests` fails, first verify Docker is available: run `docker info`. If that command fails, Docker is not running — escalate to the human rather than debugging the test failure. Do not claim success based on compilation alone.

### What not to do without approval

- Do not modify `INVARIANTS.md` constraints
- Do not add or remove dependencies in `pom.xml`
- Do not change port numbers or storage layout
- Do not add authentication, authorization, or real encryption
- Do not push to remote or open PRs

## Security

To report security vulnerabilities, see the [Security Policy](SECURITY.md).
