# Invariants for S3Mock

Non-negotiable constraints that apply to all work in this repository.
Read this before making any changes.

> Violations of these rules are **Must fix** in code review.
> Items marked *[not yet enforced]* rely on human review until tooling is added — they are the automation backlog.

---

## API Contract

✅ XML element and attribute names must match the [AWS S3 API specification](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) exactly — *Enforced by: `make integration-tests` (CI gate)*

✅ A compile check is not sufficient for XML correctness — always verify serialized output by running integration tests — *Enforced by: `make integration-tests` (CI gate)*

✅ Path-style URLs only (`http://localhost:9090/<bucket>/<key>`) — virtual-hosted-style (`bucket.localhost`) is not supported — *Enforced by: [not yet enforced]*

✅ Presigned URLs are accepted but signatures are not validated — do not add signature validation logic — *Enforced by: [not yet enforced] (by design)*

---

## Code Quality

✅ Never use AWS SDK v1 — it was removed in 5.x — *Enforced by: human review*

✅ Never use legacy Jackson XML annotations from `com.fasterxml.jackson.dataformat.xml.annotation` (e.g. `@JacksonXmlRootElement`, `@JacksonXmlProperty`) — use `tools.jackson` packages in 5.x — *Enforced by: human review*

✅ Business logic belongs in services only — controllers handle HTTP mapping, stores handle persistence; no cross-layer logic — *Enforced by: human review*

✅ Controllers never catch exceptions — `S3MockExceptionHandler` and `IllegalStateExceptionHandler` are responsible for all error responses — *Enforced by: human review*

✅ Services throw `S3Exception` constants only (`NO_SUCH_BUCKET`, `NO_SUCH_KEY`, etc.) — no custom exception classes — *Enforced by: human review*

---

## Data Integrity

✅ All dependency versions are declared in the root `pom.xml` `<properties>` section — never declare versions in sub-module POMs — *Enforced by: human review*

✅ Update the copyright year to `2017-<current year>` in the license header of every file you modify — *Enforced by: human review*

✅ Never update the copyright year in files you did not modify — copyright is bumped only when a file is actually changed — *Enforced by: human review*

---

## Security

✅ S3Mock is not for production use — it has no authentication, authorization, or real encryption — *Enforced by: [not yet enforced]*

✅ KMS key ARN format is validated, but no actual encryption is performed — do not add real KMS encryption — *Enforced by: [not yet enforced] (by implementation)*

✅ The SSL certificate is self-signed — it is not suitable for any environment other than local testing — *Enforced by: [not yet enforced] (by implementation)*

---

## Performance

No performance SLAs are defined for S3Mock. It is a local testing tool, not a throughput benchmark.

---

## Testing

✅ Never use JUnit 4 — it was removed in 5.x — *Enforced by: human review*

✅ Never mock AWS SDK clients in integration tests (`*IT.kt`) — use actual SDK v2 clients against a live S3Mock Docker container — *Enforced by: human review*

✅ Every code change must include unit tests (`*Test.kt` in `server/`) covering the new or modified logic — *Enforced by: human review*

✅ Every behavior observable via the S3 HTTP API must be covered by an integration test (`*IT.kt` in `integration-tests/`) — *Enforced by: human review*

---

## Definition of Done

A task is not complete until all of the following are true:

- Unit tests cover the new or modified logic (`*Test.kt` in the module the change was made)
- Integration tests cover the observable HTTP/S3 behavior (`*IT.kt` in `integration-tests/`)
- `CHANGELOG.md` has an entry under the current version for any user-facing bug fix or feature
- `make format` passes (ktlint + Checkstyle)
