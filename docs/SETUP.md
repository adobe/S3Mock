# Setup — S3Mock

How to build, configure, and run S3Mock locally.

## Prerequisites

| Tool | Minimum version | Notes |
|---|---|---|
| JDK | 25 | Build toolchain only — bytecode targets JDK 17 |
| Maven | 3.9+ | Or use the included `./mvnw` wrapper |
| Docker | Any recent version | Required for integration tests and `make run` |

Verify:
```bash
java -version          # Should show 25.x
./mvnw --version       # Should show 3.9+
docker info            # Should respond without error
```

## Installation

```bash
git clone https://github.com/adobe/S3Mock.git
cd S3Mock
make install           # Full build including Docker image
```

To skip Docker (faster, no image built):
```bash
make skip-docker
```

## Configuration

S3Mock is configured via environment variables (Docker / Testcontainers) or Spring properties (in-process).

| Environment variable | Spring property | Default | Description |
|---|---|---|---|
| `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT` | `com.adobe.testing.s3mock.store.root` | temp dir | Storage root directory |
| `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT` | `com.adobe.testing.s3mock.store.retainFilesOnExit` | `false` | Keep files on shutdown |
| `COM_ADOBE_TESTING_S3MOCK_STORE_REGION` | `com.adobe.testing.s3mock.store.region` | `us-east-1` | AWS region |
| `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS` | `com.adobe.testing.s3mock.store.initialBuckets` | _(none)_ | Comma-separated bucket names to create on startup |
| `COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS` | `com.adobe.testing.s3mock.store.validKmsKeys` | _(none)_ | Valid KMS ARN list (format-validated only) |
| `COM_ADOBE_TESTING_S3MOCK_CONTROLLER_CONTEXT_PATH` | `com.adobe.testing.s3mock.controller.contextPath` | `""` | Base context path for all endpoints |
| `SERVER_PORT` | `server.port` | `9191` | HTTPS port |
| `COM_ADOBE_TESTING_S3MOCK_HTTP_PORT` | `com.adobe.testing.s3mock.httpPort` | `9090` | HTTP port |

## Running the Service

```bash
make run               # Start S3Mock on HTTP :9090 / HTTPS :9191
```

Or with Docker directly:
```bash
docker run -p 9090:9090 -p 9191:9191 adobe/s3mock
```

With persistent storage:
```bash
docker run \
  -p 9090:9090 -p 9191:9191 \
  -e COM_ADOBE_TESTING_S3MOCK_STORE_ROOT=/data \
  -e COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT=true \
  -v /local/path:/data \
  adobe/s3mock
```

## Running Tests

```bash
make test                  # Unit tests only (no Docker required)
make integration-tests     # Integration tests (Docker required)
make check                 # lint + typecheck + unit tests
```

> Integration tests start a Docker container automatically via Testcontainers.

For a specific integration test:
```bash
./mvnw verify -pl integration-tests -Dit.test=BucketIT
./mvnw verify -pl integration-tests -Dit.test=BucketIT#shouldCreateBucket
```

## Running Validation

```bash
make help              # List all available targets
make lint              # ktlint + Checkstyle (check only)
make fmt               # Auto-format Kotlin (ktlint)
make typecheck         # Compile main + test sources
make sort              # Sort POM files
```

## Spring Profiles

| Profile | Effect |
|---|---|
| `debug` | Debug logging + enables actuator |
| `trace` | Trace logging + enables actuator |
| `actuator` | Enables JMX and all Spring Boot Actuator endpoints |

Activate via environment variable:
```bash
SPRING_PROFILES_ACTIVE=debug docker run -p 9090:9090 -p 9191:9191 adobe/s3mock
```

Or directly:
```bash
MANAGEMENT_ENDPOINTS_ACCESS_DEFAULT=unrestricted docker run -p 9090:9090 -p 9191:9191 adobe/s3mock
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Address already in use :9090` | Port conflict | `lsof -i :9090` then kill the process |
| Integration tests hang | Docker not running | Start Docker Desktop |
| `NoClassDefFoundError` in in-process test | Spring Boot version mismatch | Ensure your project is Spring Boot 4.x compatible |
| HTTPS connection refused | Client not trusting self-signed cert | Configure trust-all-certs on your AWS SDK client |
| Empty bucket after restart | `retainFilesOnExit` defaults to false | Set `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT=true` and use a fixed `STORE_ROOT` |
