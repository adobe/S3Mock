# Agent Context for S3Mock Server Module

> Inherits all conventions from the [root AGENTS.md](../AGENTS.md). Below are module-specific additions only.

Core S3Mock server implementation.

## Structure

```
server/src/main/kotlin/com/adobe/testing/s3mock/
├── S3MockApplication.kt       # Spring Boot entry
├── S3MockConfiguration.kt     # Top-level config
├── S3MockProperties.kt        # Properties binding
├── controller/
│   ├── *Controller.kt         # REST endpoints
│   ├── ControllerConfiguration.kt  # Controller beans + exception handlers
│   └── ControllerProperties.kt
├── dto/                       # XML/JSON models (*Result, request/response)
├── service/
│   ├── *Service.kt            # Business logic
│   └── ServiceConfiguration.kt  # Service beans (used in @SpringBootTest)
├── store/
│   ├── *Store.kt              # Persistence
│   ├── StoreConfiguration.kt  # Store beans (used in @SpringBootTest)
│   └── StoreProperties.kt
└── util/
    ├── DigestUtil.kt          # ETag/checksum computation (replaces Apache Commons)
    ├── EtagUtil.kt            # ETag normalization
    ├── HeaderUtil.kt          # HTTP header helpers
    └── AwsHttpHeaders.kt      # AWS-specific header constants
```

## Implementation Flow

**Adding S3 operation**: Follow **DTO → Store → Service → Controller → IT**:

1. **DTO** (`dto/`): Data classes with Jackson annotations — see root `AGENTS.md` § XML Serialization for the correct `tools.jackson` annotations and namespace. Verify element names against [AWS S3 API docs](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).
2. **Store** (`store/`): Filesystem path resolution, binary storage, metadata JSON. Key classes: `BucketStore`, `ObjectStore`, `BucketMetadata`, `S3ObjectMetadata`. Acquire the appropriate lock (see Locking section below).
3. **Service** (`service/`): Validation, store coordination. Throw **`S3Exception` constants** (e.g., `S3Exception.NO_SUCH_BUCKET`) — see **[docs/SPRING.md](../docs/SPRING.md)** for exception handling rules.
4. **Controller** (`controller/`): HTTP mapping only — delegate all logic to services. Controllers never catch exceptions.
5. **Integration test** (`integration-tests/`): Real AWS SDK v2 against the Docker container — see **[integration-tests/AGENTS.md](../integration-tests/AGENTS.md)**. Run `make integration-tests` to verify XML serialization against the AWS S3 API.
6. **Update docs**: `CHANGELOG.md` (user-facing entry) and root `AGENTS.md` Configuration table if new properties are added.

## Locking

Each store uses a `ConcurrentHashMap` keyed by entity identity to hold one plain `Any()` lock object per entity. All reads **and** writes of metadata files must hold the corresponding lock.

| Store | Lock key type | Lock map field | Where lock is registered |
|---|---|---|---|
| `BucketStore` | `String` (bucket name) | `lockStore` | `createBucket` / `loadBuckets` via `lockStore.putIfAbsent(bucketName, Any())` |
| `ObjectStore` | `UUID` (object ID) | `lockStore` | Before first write via `lockStore.putIfAbsent(id, Any())` |
| `MultipartStore` | `UUID` (upload ID) | `lockStore` | On upload creation via `lockStore.putIfAbsent(uploadId, Any())` |

**Pattern for adding a store method that reads or writes metadata:**

```kotlin
// Register lock lazily (writes only — reads rely on the lock already existing)
lockStore.putIfAbsent(id, Any())
// Acquire lock
synchronized(lockStore[id]!!) {
    // read or write metadata here
}
```

**Rules:**
- Never skip the lock for reads — `getBucketMetadata` and `getS3ObjectMetadata` are also synchronized.
- Never acquire more than one lock in a single call path — there is no established ordering, so taking two locks risks deadlock.
- Do not introduce `ReentrantLock`, `ReadWriteLock`, or other lock types — the existing `synchronized`/`Any()` pattern is intentional and consistent throughout all stores.

## Storage Schema

Filesystem layout:
```
<root>/<bucket>/bucketMetadata.json
<root>/<bucket>/<uuid>/binaryData
<root>/<bucket>/<uuid>/objectMetadata.json
<root>/<bucket>/<uuid>/<version-id>-binaryData              # versioning
<root>/<bucket>/<uuid>/<version-id>-objectMetadata.json      # versioning
<root>/<bucket>/multiparts/<upload-id>/multipartMetadata.json
<root>/<bucket>/multiparts/<upload-id>/<part-number>.part
<root>/<bucket>/multiparts/<upload-id>/<part-number>.partmeta.json  # per-part checksum + size
```

**`bucketMetadata.json`** fields (`BucketMetadata`):

| Field | Type | Notes |
|---|---|---|
| `name` | `String` | Bucket name |
| `creationDate` | `String` | ISO-8601 timestamp |
| `bucketRegion` | `String` | AWS region string |
| `objects` | `Map<String, UUID>` | key → object UUID mapping |
| `versioningConfiguration` | `VersioningConfiguration?` | null until versioning is configured |
| `objectLockConfiguration` | `ObjectLockConfiguration?` | null until object lock is enabled |
| `bucketLifecycleConfiguration` | `BucketLifecycleConfiguration?` | null until lifecycle rules are set |
| `objectOwnership` | `ObjectOwnership?` | null until ownership is set |
| `bucketInfo` | `BucketInfo?` | bucket type/data-redundancy info |
| `locationInfo` | `LocationInfo?` | bucket location info |
| `path` | `Path` | filesystem path to the bucket folder (serialized, but not portable across hosts or filesystem layouts) |

**`objectMetadata.json`** fields (`S3ObjectMetadata`):

| Field | Type | Notes |
|---|---|---|
| `id` | `UUID` | object identity (matches the folder name) |
| `key` | `String` | S3 object key |
| `size` | `String` | content length as string |
| `contentType` | `String?` | MIME type |
| `etag` | `String?` | ETag value |
| `modificationDate` | `String` | formatted date string |
| `lastModified` | `Long` | epoch millis |
| `dataPath` | `Path` | path to the `binaryData` file |
| `userMetadata` | `Map<String, String>?` | `x-amz-meta-*` headers |
| `storeHeaders` | `Map<String, String>?` | headers persisted verbatim (e.g. `Content-Encoding`) |
| `encryptionHeaders` | `Map<String, String>?` | SSE headers |
| `tags` | `List<Tag>?` | object tags |
| `checksumAlgorithm` | `ChecksumAlgorithm?` | CRC32 / SHA-256 / etc. |
| `checksum` | `String?` | computed checksum value |
| `checksumType` | `ChecksumType?` | FULL\_OBJECT or COMPOSITE |
| `storageClass` | `StorageClass?` | STANDARD, GLACIER, etc. |
| `owner` | `Owner` | object owner |
| `legalHold` | `LegalHold?` | WORM legal hold status |
| `retention` | `Retention?` | WORM retention mode + until-date |
| `policy` | `AccessControlPolicy?` | ACL policy |
| `versionId` | `String?` | non-null when versioning is enabled |
| `deleteMarker` | `Boolean` | true for versioned delete markers |
| `parts` | `List<ObjectPart>?` | per-part metadata for multipart-completed objects; null for single-PUT objects; populated at `CompleteMultipartUpload` from `.partmeta.json` sidecar files |

**`<part-number>.partmeta.json`** fields (`PartMetadata`) — written alongside each `.part` file during `UploadPart`; deleted after `CompleteMultipartUpload`:

| Field | Type | Notes |
|---|---|---|
| `partNumber` | `Int` | S3 part number (1-based) |
| `etag` | `String?` | ETag of the part |
| `size` | `Long` | part size in bytes |
| `lastModified` | `Long` | epoch millis when the part was uploaded |
| `checksum` | `String?` | base64-encoded checksum value, or null if not provided |
| `checksumAlgorithm` | `ChecksumAlgorithm?` | algorithm used, or null |

## Testing

See **[docs/TESTING.md](../docs/TESTING.md)** for the full strategy. Service and store tests use `@SpringBootTest` with `@MockitoBean`; controller tests use `@WebMvcTest` with `@MockitoBean` and `BaseControllerTest`. Always extend the appropriate base class (`ServiceTestBase`, `StoreTestBase`, `BaseControllerTest`).

## Configuration

Three `@ConfigurationProperties` classes bind environment variables to typed properties:
- `StoreProperties` (`com.adobe.testing.s3mock.store.*`) — storage root, buckets, KMS, region
- `ControllerProperties` (`com.adobe.testing.s3mock.controller.*`) — context path
- `S3MockProperties` (`com.adobe.testing.s3mock.*`) — top-level settings

**When adding, renaming, or removing a property**, you must also update the testsupport modules that expose it to users:
- `testsupport/testcontainers/` — add/update a `withX()` method and `PROP_X` env var constant in `S3MockContainer` (uppercase Spring key, replace `.` with `_`)
- `testsupport/common/` — add/update a `withX()` method and `PROP_X` constant in `S3MockStarter` (Spring key form)
- `AGENTS.md` (root) — update the Configuration section env var table if the property is user-facing
- `README.md` — update the configuration table if the property is user-facing

