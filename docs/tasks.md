# S3Mock Improvement Tasks

This document contains a list of potential improvements for the S3Mock project. Each task is marked with a checkbox that can be checked off when completed.

## Architecture Improvements

1. [ ] Implement support for domain-style access to buckets (e.g., `http://bucket.localhost:9090/someKey`) to better match AWS S3 behavior
2. [ ] Add support for AWS S3 operations that are currently not implemented (e.g., bucket policies, website configuration, etc.)
3. [ ] Implement a plugin architecture to allow for custom extensions and behaviors
4. [ ] Add support for S3 event notifications (SNS, SQS, Lambda)
5. [ ] Improve error handling to more closely match AWS S3 error responses
6. [ ] Implement a metrics collection system to monitor performance and usage
7. [ ] Add support for S3 Select for querying object content
8. [ ] Implement support for S3 Inventory reports
9. [ ] Add support for S3 Analytics configurations
10. [ ] Implement S3 Batch Operations

## Code Quality Improvements

11. [ ] Increase unit test coverage for controller, service and store layers

### Task 11 – Test Coverage Plan (Components and Actions)

Scope and components to cover:
- Controllers (HTTP): BucketController, ObjectController, MultipartController
- Services (business logic): BucketService, ObjectService, MultipartService, Kms* services
- Stores (persistence): BucketStore, ObjectStore, MultipartStore, KmsKeyStore
- XML/DTOs and mappers: request/response XML models, serialization utils
- Utilities: digest, ETag, headers, Range requests, SSE
- Configuration: StoreConfiguration, controller advice/error mapping

Priorities (short-to-long horizon):
1) High-value happy-path and error-path coverage for controllers with mocked services (fast feedback).
2) Store layer correctness with Spring Boot WebEnvironment.NONE tests (file IO, metadata persistence, edge cases).
3) Service layer behavior with mocked stores (parameter validation, branching, SSE/KMS interactions).
4) XML serialization/deserialization fidelity for commonly used operations.
5) Regression tests for known corner cases (range requests, conditional headers, multipart completion ordering, KMS key validation).

Concrete test additions (incremental):
- Controllers
  - BucketController
    - listBuckets returns empty and non-empty results; XML schema shape
    - createBucket duplicate name -> proper S3 error code
    - deleteBucket non-empty -> proper error
  - ObjectController
    - putObject with/without Content-MD5; mismatched MD5 -> error
    - getObject with Range header (single range) -> 206 + Content-Range
    - getObject nonexistent -> 404 S3-style error
    - headObject verifies metadata and headers (ETag, Content-Length)
  - MultipartController
    - initiateMultipartUpload returns uploadId
    - uploadPart with invalid partNumber -> error mapping
    - completeMultipartUpload out-of-order parts -> consistent ETag behavior
- Services
  - ObjectService.storeObject validates metadata, handles SSE headers routing to KMS
  - BucketService.deleteBucket checks emptiness guard
- Stores
  - ObjectStore
    - storeS3ObjectMetadata and getS3ObjectMetadata roundtrip
    - list with prefix/delimiter, max-keys, continuation
    - delete removes metadata and data file
  - BucketStore
    - create, list, delete, exist checks
  - MultipartStore
    - init, addPart, complete, abort state transitions
- XML/DTOs
  - Serialize/deserialize ListAllMyBucketsResult, CompleteMultipartUploadResult

Suggested file locations (server module):
- Controllers: server/src/test/kotlin/com/adobe/testing/s3mock/itlike/controller/*Test.kt (extending BaseControllerTest)
- Services: server/src/test/kotlin/com/adobe/testing/s3mock/service/*Test.kt
- Stores: server/src/test/kotlin/com/adobe/testing/s3mock/store/*Test.kt (extend StoreTestBase)
- DTOs: server/src/test/kotlin/com/adobe/testing/s3mock/xml/*Test.kt

Execution (fast path per repo guidelines):
- One test class: ./mvnw -pl server test -Dtest=ObjectStoreTest
- One method:   ./mvnw -pl server test -Dtest=ObjectStoreTest#testStoreAndGetObject
- Or via tool:  run_test server/src/test/kotlin/com/adobe/testing/s3mock/store/ObjectStoreTest.kt

Acceptance targets for Task 11 completion:
- +10–15% line coverage increase in server module, focusing on controllers and stores
- At least one new test per component category listed above
- Error-path assertions include correct HTTP status and S3 error codes/messages

Notes:
- Avoid ITs unless Docker available; prefer WebEnvironment.RANDOM_PORT controller tests with mocked services.
- Use provided test bases: BaseControllerTest, StoreTestBase, ServiceTestBase.
- Reuse existing sample files: server/src/test/resources/sampleFile.txt, sampleFile_large.txt, sampleKMSFile.txt.
12. [ ] Refactor synchronization mechanisms in store classes to improve concurrency handling
13. [ ] Implement more comprehensive input validation for S3 API parameters
14. [ ] Add more detailed logging throughout the application for better debugging
15. [ ] Refactor XML serialization/deserialization to use more type-safe approaches
16. [ ] Improve exception handling and error messages
17. [ ] Implement more robust cleanup of temporary files
18. [ ] Add comprehensive JavaDoc to all public APIs
19. [ ] Refactor controller classes to reduce code duplication
20. [ ] Implement more thorough validation of S3 object metadata

## Performance Improvements

21. [x] Optimize file storage for large objects
22. [ ] Implement caching for frequently accessed objects
23. [ ] Optimize list operations for buckets with many objects
24. [ ] Improve multipart upload performance
25. [x] Reduce memory usage when handling large files
26. [ ] Optimize XML serialization/deserialization
27. [x] Keep object metadata storage as plain text (JSON) for inspectability (decided against more efficient/binary storage)
28. [ ] Add support for conditional requests to reduce unnecessary data transfer
29. [ ] Optimize concurrent access patterns
30. [ ] Implement more efficient bucket and object locking mechanisms

## Security Improvements

31. [ ] Add support for bucket policies
32. [ ] Implement proper authentication and authorization mechanisms
33. [ ] Add support for AWS IAM-style access control
34. [ ] Implement proper handling of AWS signatures for authentication
35. [ ] Add support for server-side encryption with customer-provided keys
36. [ ] Improve SSL/TLS configuration and certificate management
37. [ ] Implement proper input sanitization to prevent injection attacks
38. [ ] Add support for VPC endpoint policies
39. [ ] Implement proper access logging
40. [ ] Add support for AWS CloudTrail-style audit logging

## Documentation Improvements

41. [ ] Create comprehensive API documentation
42. [ ] Add more examples for different usage scenarios
43. [ ] Improve README with more detailed setup instructions
44. [ ] Create troubleshooting guide
45. [ ] Add architecture diagrams
46. [ ] Document internal design decisions
47. [ ] Create contributor guidelines
48. [ ] Add more code examples for different programming languages
49. [ ] Create a user guide with common use cases
50. [ ] Document performance characteristics and limitations

## DevOps Improvements

51. [ ] Add Docker Compose examples for different scenarios
52. [ ] Implement health checks for container orchestration
53. [ ] Add Kubernetes deployment examples
54. [ ] Improve CI/CD pipeline
55. [ ] Add performance benchmarking to CI process
56. [ ] Implement automated security scanning
57. [ ] Add support for configuration through environment variables for all settings
58. [ ] Improve Docker image size and build time
59. [ ] Add support for container orchestration tools
60. [ ] Implement graceful shutdown and startup procedures

## Dependency and Technology Updates

61. [ ] Evaluate migration to Java 21 for improved performance and features
62. [ ] Update to latest Spring Boot version when available
63. [ ] Consider using reactive programming model with Spring WebFlux
64. [ ] Evaluate using non-blocking I/O for file operations
65. [ ] Consider using a more efficient serialization format for metadata storage
66. [ ] Evaluate using a database for metadata storage instead of files
67. [ ] Update AWS SDK dependencies regularly
68. [ ] Consider using GraalVM native image compilation for faster startup
69. [ ] Evaluate using Project Loom virtual threads for improved concurrency
70. [ ] Consider using modern Java language features more extensively (records, sealed classes, etc.)

## Testing Improvements

71. [ ] Add more integration tests for edge cases
72. [ ] Implement property-based testing for robust validation
73. [ ] Add performance regression tests
74. [ ] Implement chaos testing for resilience verification
75. [ ] Add more unit tests for utility classes
76. [ ] Implement contract tests for S3 API compatibility
77. [ ] Add tests for concurrent access scenarios
78. [ ] Improve test coverage for error conditions
79. [ ] Add tests for different storage backends
80. [ ] Implement automated compatibility testing with different AWS SDK versions

## User Experience Improvements

81. [ ] Add a web-based UI for browsing buckets and objects
82. [ ] Implement a CLI tool for interacting with S3Mock
83. [ ] Add better error messages and troubleshooting information
84. [ ] Improve startup time
85. [ ] Add support for custom endpoints
86. [ ] Implement request/response logging for debugging
87. [ ] Add support for request tracing
88. [ ] Improve configuration options documentation
89. [ ] Add support for programmatic configuration
90. [ ] Implement better defaults for common use cases
