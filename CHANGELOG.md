# Changelog

## 2.x
* Features and fixes
  * TBD
* Refactorings
  * Add [Maven Wrapper](https://maven.apache.org/wrapper) for reliable builds
* Version updates
  * TBD

## 2.4.14
* Features and fixes
  * Fix broken SSL handshake (fixes #706)
  * Fix KMS key handling (fixes #702)
  * Allow listing a MultiPartUpload with no parts (fixes #694)
  * Update SSL certificate with san=dns:localhost (fixes #228)
* Refactorings
  * Add sortpom-maven-plugin, run sortpom
    * To run manually, execute `mvn com.github.ekryd.sortpom:sortpom-maven-plugin:sort`
  * Enable Kotlin compiler for integration-tests only.
* Version updates
  * Bump alpine from 3.16.0 to 3.16.1 in /docker
  * Bump aws-java-sdk-s3 from 1.12.240 to 1.12.267
  * Bump aws-v2.version from 2.17.211 to 2.17.239
  * Bump spring-boot.version from 2.7.0 to 2.7.2
  * Bump kotlin.version from 1.7.0 to 1.7.10
  * Bump testng from 7.6.0 to 7.6.1
  * Bump testcontainers.version from 1.17.2 to 1.17.3
  * Bump checkstyle from 10.3 to 10.3.1
  * Bump maven-deploy-plugin from 3.0.0-M2 to 3.0.0
  * Bump docker-maven-plugin from 0.40.0 to 0.40.1
  * Bump sortpom-maven-plugin from 3.1.3 to 3.2.0
  * Bump maven-install-plugin from 3.0.0-M1 to 3.0.1
  * Bump maven-resources-plugin from 3.2.0 to 3.3.0
  * Bump exec-maven-plugin from 3.0.0 to 3.1.0

## 2.4.13
* Features and fixes
  *  Adds missing x-amz-server-side-encryption and x-amz-server-side-encryption-aws-kms-key-id header (fixes #639)
* Version updates
  * Bump alpine from 3.15.4 to 3.16.0 in /docker
  * Bump aws-java-sdk-s3 from 1.12.212 to 1.12.240
  * Bump aws-v2.version from 2.17.182 to 2.17.211
  * Bump spring-boot.version from 2.6.7 to 2.7.0
  * Bump kotlin.version from 1.6.21 to 1.7.0
  * Bump testng from 7.5 to 7.6.0
  * Bump docker/setup-qemu-action from 1 to 2
  * Bump checkstyle from 10.2 to 10.3
  * Bump docker-maven-plugin from 0.39.1 to 0.40.0
  * Bump maven-failsafe-plugin from 3.0.0-M6 to 3.0.0-M7
  * Bump maven-surefire-plugin from 3.0.0-M6 to 3.0.0-M7
  * Bump maven-release-plugin from 3.0.0-M5 to 3.0.0-M6
  * Bump maven-enforcer-plugin from 3.0.0 to 3.1.0

## 2.4.11 - 2.4.12
Test releases that incremented the version number but were not released to Maven One or Docker Hub.
Please refer / update to version 2.4.13, thanks.

## 2.4.10
* Features and fixes
  * ListParts returns 404 on unknown upload id (fixes #475)
  * Allow CopyObject of key to same key in the same bucket (fixes #468)
  * Fix handling of userMetadata in CopyObject REPLACE and COPY cases (fixes #468 again, fixes #569)
  * Add multipart tests for "small" last part (fixes #524)
* Refactorings
  * Migrate integration tests to kotlin
* Version updates
  * Bump alpine from 3.15.0 to 3.15.4 in /docker
  * Bump aws-java-sdk-s3 from 1.12.170 to 1.12.212
  * Bump aws-v2.version from 2.17.141 to 2.17.182
  * Bump spring-boot.version from 2.6.3 to 2.6.7
  * Bump spring-security-oauth2 from 2.5.1.RELEASE to 2.5.2.RELEASE
  * Bump testcontainers.version from 1.16.3 to 1.17.1
  * Bump maven-surefire-plugin from 3.0.0-M5 to 3.0.0-M6
  * Bump maven-failsafe-plugin from 3.0.0-M5 to 3.0.0-M6
  * Bump maven-compiler-plugin from 3.10.0 to 3.10.1
  * Bump maven-clean-plugin from 3.1.0 to 3.2.0
  * Bump maven-dependency-plugin from 3.2.0 to 3.3.0
  * Bump kotlin.version from 1.6.10 to 1.6.21
  * Bump maven-javadoc-plugin from 3.3.2 to 3.4.0
  * Bump checkstyle from 9.3 to 10.2

## 2.4.9
* Features and fixes
  * Verify source key exists on CopyObject and CopyObjectPart (fixes #459)
* Refactorings
  * Tagging does not have a "versionId" property in S3 API.
  * CompleteMultipartUpload children are of type "CompletedPart".
  * BatchDeleteResponse children are of type "DeletedObject".
  * DeleteResult contains Error elements for failed deletes.
  * Fixed JavaDoc of various classes and methods
  * Add deprecation notice / documentation (fixes #418)
  * Remove unnecessary methods and constructors
  * Rename classes to better match their counterpart in S3 API
  * Convert Integration Test to Kotlin
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.162 to 1.12.170
  * Bump aws-v2.version from 2.17.133 to 2.17.141
  * Bump docker-maven-plugin from 0.39.0 to 0.39.1

## 2.4.8
* Features and fixes
  * Let S3Mock run with custom application.properties on classpath (fixes #434)
  * Change Docker image entrypoint to exec form (fixes #421)
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.150 to 1.12.162
  * Bump aws-v2.version from 2.17.120 to 2.17.133
  * Bump docker-maven-plugin from 0.38.1 to 0.39.0
  * Bump maven-javadoc-plugin from 3.3.1 to 3.3.2
  * Bump maven-compiler-plugin from 3.9.0 to 3.10.0

## 2.4.7
* Features and fixes
  * getObjectTagging incorrectly returns JSON instead of XML (fixes #406)

## 2.4.6
* Features and fixes
  * Docker image is now available as multi arch for both `amd64` and `arm64` platforms. (fixes #253, #287, #313, #389)

## 2.4.2 - 2.4.5

Test releases that incremented the version number but were not released to Maven One or Docker Hub.
Please refer / update to version 2.4.6, thanks.

## 2.4.1

* Features and fixes
  * Make contextPath of FileStoreController configurable (Fixes #388)
  * Add multi-part upload checks, S3 has a minimum size allowed of 5MB (Fixes #392)
  * Handle Content-MD5 header. (Fixes #208)
  * Etags are Hex encoded digests. (Fixes #208)
* Refactorings
  * Introduce @Configuration for packages.
  * Move all remaining DTOs to "dto" package.
  * Rename "domain" package to "store".
  * Reduced visibility of some classes and methods to package / private.
  * Use Java 17 for CI and release.
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.131 to 1.12.150
  * Bump aws-v2.version from 2.17.102 to 2.17.120
  * Bump xmlunit-assertj3 from 2.8.4 to 2.9.0
  * Bump spring-boot.version from 2.6.2 to 2.6.3
  * Bump testcontainers.version from 1.16.2 to 1.16.3
  * Bump maven-compiler-plugin from 3.8.1 to 3.9.0
  * Bump maven-release-plugin from 3.0.0-M4 to 3.0.0-M5
  * Bump maven-jar-plugin from 3.2.0 to 3.2.2
  * Bump testng from 7.4.0 to 7.5

## 2.4.0

Problems publishing through Sonatype, only Docker container was released, not the Java libraries.

Please refer / update to version 2.4.1, thanks.

## 2.3.3

* Features and fixes
  * Fix MultipartUpload without range (fixes #341)
  * Treat empty delimiter as `null` (fixes #306)
  * Fix -DskipDocker (fixes #344)
* Version updates
  * Bump maven-deploy-plugin from 3.0.0-M1 to 3.0.0-M2
  * Bump spring-boot.version from 2.6.1 to 2.6.2
  * Bump aws-java-sdk-s3 from 1.12.130 to 1.12.131
  * Bump checkstyle from 9.2 to 9.2.1
  
## 2.3.2
* Features and fixes
  * Set only one CORS header (fixes #74 - again)
  * Add examples for validKmsKeys and initialBuckets configuration (fixes #322)
* Refactorings
  * Using official Alpine Docker container with JDK17 from APK to run
    * [alpine:3.15.0](https://hub.docker.com/_/alpine)
    * This is in preparation of multi-arch release
* Added dependabot for automated version updates
* Version updates
  * Bump spring-boot from 2.3.12.RELEASE to 2.6.1
  * Bump aws-v2 from 2.17.73 to 2.17.102
  * Bump aws-java-sdk-s3 from 1.12.15 to 1.12.130
  * Bump commons-io from 2.10.0 to 2.11.0
  * Bump jaxb-api from 2.3.0 to 2.3.1
  * Bump checkstyle from 8.44 to 9.2
  * Bump xmlunit-assertj3 from 2.8.2 to 2.8.4
  * Bump maven-resources-plugin from 3.1.0 to 3.2.0
  * Bump maven-checkstyle-plugin from 3.1.1 to 3.1.2
  * Bump maven-enforcer-plugin from 3.0.0-M3 to 3.0.0
  * Bump maven-javadoc-plugin from 3.2.0 to 3.3.1
  * Bump docker-maven-plugin from 0.36.1 to 0.38.1
  * Bump Log4j2 to 2.15.0 (not actively used, just in case)

## 2.3.1

Problems publishing through Sonatype, only Docker container was released, not the libraries.

Please refer / update to version 2.3.2, thanks.

## 2.3.0

Problems publishing through Sonatype, only Docker container was released, not the libraries.

Please refer / update to version 2.3.2, thanks.

## 2.2.3

* Features and fixes
  * Set bucket for multipart uploads, return by bucket. (Fixes #292)

## 2.2.2

* Features and fixes
  * Adds option "retainFilesOnExit" to keep files after exit. Default is to remove all files. (Fixes #286)
  * Fixes ignored "root" environment variable in Docker (Fixes #289)
  * Support CORS headers from any host (fixes #74)

## 2.2.1

* Features and fixes
  * Fixes copy part / copy object encoding issues (Fixes #279)

## 2.2.0

* Features and fixes
  * Add TestContainers support

## 2.1.35

* Features and fixes
  * Delete all files on JVM shutdown (Fixes #249)
* Refactorings
  * Extract Docker build and integration tests to separate modules
  * Docker build and integration test is now optional, run with "-DskipDocker" to skip the Docker build and the integration tests. (Fixes #235)

## 2.1.34

* Features and fixes
  * ETag value now enclosed in quotation marks
  * All dates are formatted in UTC timezone (Fixes #203)
  * "CommonPrefixes" are now serialized als multiple elements containing one "Prefix" (Fixes #215)
  * Removed several superfluous / erroneous elements like "truncated" or "part" from various responses

## 2.1.33

* Features and fixes
  * Fixed potential NPE in FileStore
* Version updates
  * Updated spring-boot to 2.3.12.RELEASE
  * Updated aws-java-sdk-s3 to 1.12.15
  * Updated awssdk V2 to 2.16.93
  * Updated commons-codec to 1.15
  * Updated commons-io to 2.10.0
  * Updated junit-jupiter to 5.7.0
  * Updated testng to 7.4.0
  * Updated base Docker image to alpine-3.13_glibc-2.33
  * Updated JDK bundled in Docker image to 11.0.11_9
  * Removed unneeded junit-bom import
  * Updated checkstyle to 8.44

## 2.1.32

* Features and fixes
  * Fixes getS3Object with absolute path (Fixes #245 and #248)

## 2.1.31
* 
* Version updates
  * Updated spring-security-oauth2 from 2.3.5.RELEASE to 2.3.6.RELEASE

## 2.1.30

* Features and fixes
  * Fix encoded responses for aws-cli (Fixes #257)
* Version updates
  * Updated commons-io from 2.6 to 2.7

## 2.1.29

* Features and fixes
  * Add encodingType as return parameter of ListBucket and ListBucketV2
* Version updates
  * Updated junit from 4.13-beta-1 to 4.13.1

## 2.1.28

* Refactorings
  * Changes to build system, test release

## 2.1.27

* Features and fixes
  * Remove accidental JDK9+ bytecode dependency (Fixes #243)

## 1.0.0

Initial Release
