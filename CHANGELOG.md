# Changelog

S3Mock follows [Semantic Versioning](https://semver.org/). It depends on lots of 3rd party libraries which are updated regularly.
Whenever a 3rd party library is updated, S3Mock will update it's MINOR version.

<!-- TOC -->
* [Changelog](#changelog)
* [PLANNED - 6.x - RELEASE TBD](#planned---6x---release-tbd)
  * [Planned changes](#planned-changes)
* [CURRENT - 5.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT](#current---5x---this-version-is-under-active-development)
  * [5.0.0](#500)
* [DEPRECATED - 4.x](#deprecated---4x)
  * [4.11.0](#4110)
  * [4.10.0](#4100)
  * [4.9.1](#491)
  * [4.9.0](#490)
  * [4.8.0](#480)
  * [4.7.0](#470)
  * [4.6.0](#460)
  * [4.5.1](#451)
  * [4.5.0](#450)
  * [4.4.0](#440)
  * [4.3.0](#430)
  * [4.2.0](#420)
  * [4.1.1](#411)
  * [4.1.0](#410)
  * [4.0.0](#400)
* [DEPRECATED - 3.x](#deprecated---3x)
  * [3.12.0](#3120)
  * [3.11.0](#3110)
  * [3.10.3](#3103)
  * [3.10.2](#3102)
  * [3.10.1](#3101)
  * [3.10.0](#3100)
  * [3.9.1](#391)
  * [3.9.0](#390)
  * [3.8.0](#380)
  * [3.7.3](#373)
  * [3.7.2](#372)
  * [3.7.1](#371)
  * [3.7.0](#370)
  * [3.6.0](#360)
  * [3.5.2](#352)
  * [3.5.1](#351)
  * [3.5.0](#350)
  * [3.4.0](#340)
  * [3.3.1](#331)
  * [3.3.0](#330)
  * [3.2.0](#320)
  * [3.1.0](#310)
  * [3.0.1](#301)
  * [3.0.0](#300)
* [DEPRECATED - 2.x](#deprecated---2x)
  * [2.17.0](#2170)
  * [2.16.0](#2160)
  * [2.15.1](#2151)
  * [2.15.0](#2150)
  * [2.14.0](#2140)
  * [2.13.1](#2131)
  * [2.13.0](#2130)
  * [2.12.2](#2122)
  * [2.12.1](#2121)
  * [2.12.0](#2120)
  * [2.11.0](#2110)
  * [2.10.2](#2102)
  * [2.10.1](#2101)
  * [2.10.0](#2100)
  * [2.9.1](#291)
  * [2.9.0](#290)
  * [2.8.0](#280)
  * [2.7.2](#272)
  * [2.7.1](#271)
  * [2.7.0](#270)
  * [2.6.3](#263)
  * [2.6.2](#262)
  * [2.6.1](#261)
  * [2.6.0](#260)
  * [2.5.4](#254)
  * [2.5.3](#253)
  * [2.5.2](#252)
  * [2.5.1](#251)
  * [2.5.0](#250)
  * [2.4.16](#2416)
  * [2.4.15](#2415)
  * [2.4.14](#2414)
  * [2.4.13](#2413)
  * [2.4.11 - 2.4.12](#2411---2412)
  * [2.4.10](#2410)
  * [2.4.9](#249)
  * [2.4.8](#248)
  * [2.4.7](#247)
  * [2.4.6](#246)
  * [2.4.2 - 2.4.5](#242---245)
  * [2.4.1](#241)
  * [2.4.0](#240)
  * [2.3.3](#233)
  * [2.3.2](#232)
  * [2.3.1](#231)
  * [2.3.0](#230)
  * [2.2.3](#223)
  * [2.2.2](#222)
  * [2.2.1](#221)
  * [2.2.0](#220)
  * [2.1.35](#2135)
  * [2.1.34](#2134)
  * [2.1.33](#2133)
  * [2.1.32](#2132)
  * [2.1.31](#2131-1)
  * [2.1.30](#2130-1)
  * [2.1.29](#2129)
  * [2.1.28](#2128)
  * [2.1.27](#2127)
* [DEPRECATED - 1.x](#deprecated---1x)
  * [1.0.0](#100)
<!-- TOC -->

# PLANNED - 6.x - RELEASE TBD
Version 6.x is JDK25 LTS bytecode compatible, with Docker integration.

Will be released after Spring Boot 5.x, updating baselines etc. as Spring Boot 5.x requires.

Any JUnit / direct Java usage support will most likely be dropped and only supported on a best-effort basis.
(i.e., the modules will be deleted from the code base and not released anymore. It *may* be possible to
still run S3Mock directly in Java.)
The S3Mock is a Spring Boot application and currently contains various workarounds to make it possible
to easily to run `S3MockApplication#start` from a static context. These workarounds will be deleted.

Running S3Mock in unit tests is still supported by using [TestContainers](https://www.testcontainers.org/).

**Once 6.x is released, 5.x may receive bug fixes and features. This will be best-effort only.**

## Planned changes

* Features and fixes
  * TBD
* Refactorings
  * Looking to Remove unit test modules. This enables
    * Refactoring S3Mock to a "standard" Spring Boot application.
    * Removal of workarounds to use `S3MockApplication#start` from a static context
* Version updates
  * Bump Spring Boot version to 5.0.0
  * Bump Spring Framework version to 8.0.0

# CURRENT - 5.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT
Version 5.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**The current major version 5 will receive new features, dependency updates and bug fixes on a continuous basis. We usually follow the Spring Boot release cycle.**

## 5.0.0

* Features and fixes
  * Get object with range now returns the same headers as non-range calls.
  * Docker: Copy "s3mock.jar" to "/opt/", run with absolute path reference to avoid issues when working directory is changed. (fixes #2827)
  * S3Mock supports ChecksumType.FULL_OBJECT for Multipart uploads (fixes #2843)
  * Return 412 on if-none-match=true when making CompleteMultipartRequest (fixes #2790) 
* Refactorings
  * Use Jackson 3 annotations and mappers.
  * AWS has deprecated SDK for Java v1 and will remove support EOY 2025.
    * Remove Java SDK v1.
  * JUnit 4.x deprecation
    * Remove JUnit 4.x support.
  * Remove legacy properties for S3Mock configuration.
  * Move all controller-related code from "com.adobe.testing.s3mock" to "com.adobe.testing.s3mock.controller" package.
  * Remove Apache libraries like "commons-compress", "commons-codec" or "commons-lang3" from dependencies. Kotlin and Java standard library provide similar functionality.
* Version updates (deliverable dependencies)
  * Bump Spring Boot version to 4.0.0
  * Bump Spring Framework version to 7.0.1
  * Bump java version from 17 to 25
    * Compile with Java 25, target Java 17
    * Docker container runs Java 25
  * Bump TestContainers to 2.0.2
* Version updates (build dependencies)
  * Bump Maven to 4.0.0
  * Bump org.apache.maven.plugins:maven-release-plugin from 3.3.0 to 3.3.1
  * Bump com.puppycrawl.tools:checkstyle from 12.2.0 to 12.3.0
  * Bump actions/upload-artifact from 5.0.0 to 6.0.0
  * Bump github/codeql-action from 4.31.6 to 4.31.8
  * Bump actions/setup-java from 5.0.0 to 5.1.0
  * Bump step-security/harden-runner from 2.13.3 to 2.14.0

# DEPRECATED - 4.x
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**4.x is DEPRECATED and may receive bug fixes and features. This will be best-effort only.**

## 4.11.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**This is the last planned minor release of 4.x.**

* Features and fixes
  *  Support quiet parameter in DeleteObjects (fixes #2756)
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.5.7 to 3.5.8
  * Bump aws-v2.version from 2.38.1 to 2.40.0
  * Bump aws.version from 1.12.793 to 1.12.794
  * Bump org.apache.commons:commons-lang3 from 3.19.0 to 3.20.0
  * Bump alpine from 3.22.2 to 3.23.0 in /docker
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.5.77 to 1.5.95
  * Bump io.fabric8:docker-maven-plugin from 0.47.0 to 0.48.0
  * Bump org.apache.maven.plugins:maven-release-plugin from 3.1.1 to 3.3.0
  * Bump org.apache.maven.plugins:maven-jar-plugin from 3.4.2 to 3.5.0
  * Bump org.apache.maven.plugins:maven-source-plugin from 3.3.1 to 3.4.0
  * Bump org.apache.maven.plugins:maven-resources-plugin from 3.3.1 to 3.4.0
  * Bump actions/stale from 10.1.0 to 10.1.1
  * Bump actions/dependency-review-action from 4.8.1 to 4.8.2
  * Bump com.puppycrawl.tools:checkstyle from 12.1.1 to 12.2.0
  * Bump actions/checkout from 5.0.0 to 6.0.1
  * Bump github/codeql-action from 4.31.2 to 4.31.6
  * Bump step-security/harden-runner from 2.13.2 to 2.13.3
  * Bump maven wrapper from 3.3.3 to 3.3.4

## 4.10.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Return correct error body on invalid ranges (fixes #2732)
  * Accept unquoted etags in if-match/if-none-match headers (fixes #2665)
  * StoreCleaner deletes files on exit (fixes #2745)
* Refactorings
  * Drop commons-lang3 dependency and replace its usages with core Java (fixes #2735)
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.5.6 to 3.5.7
  * Bump aws-v2.version from 2.33.12 to 2.38.1
  * Bump aws.version from 1.12.791 to 1.12.793
  * Bump alpine from 3.22.1 to 3.22.2 in /docker
  * Bump commons-codec:commons-codec from 1.19.0 to 1.20.0
  * Bump commons-io:commons-io from 2.20.0 to 2.21.0
* Version updates (build dependencies)
  * Bump kotlin.version from 2.2.20 to 2.2.21
  * Bump aws.sdk.kotlin:s3-jvm from 1.5.41 to 1.5.77
  * Bump io.fabric8:docker-maven-plugin from 0.46.0 to 0.47.0
  * Bump digital.pragmatech.testing:spring-test-profiler from 0.0.12 to 0.0.14
  * Bump org.mockito.kotlin:mockito-kotlin from 6.0.0 to 6.1.0
  * Bump org.xmlunit:xmlunit-assertj3 from 2.10.4 to 2.11.0
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.5.1 to 3.6.2
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.11.3 to 3.12.0
  * Bump org.apache.maven.plugins:maven-compiler-plugin from 3.14.0 to 3.14.1
  * Bump org.apache.maven.plugins:maven-dependency-plugin from 3.8.1 to 3.9.0
  * Bump org.apache.maven.plugins:maven-enforcer-plugin from 3.6.1 to 3.6.2
  * Bump com.puppycrawl.tools:checkstyle from 11.0.1 to 12.1.1
  * Bump org.jacoco:jacoco-maven-plugin from 0.8.13 to 0.8.14
  * Bump github/codeql-action from 3.30.3 to 4.31.2
  * Bump actions/dependency-review-action from 4.7.3 to 4.8.1
  * Bump ossf/scorecard-action from 2.4.2 to 2.4.3
  * Bump actions/stale from 10.0.0 to 10.1.0
  * Bump actions/upload-artifact from 4.6.2 to 5.0.0
  * Bump step-security/harden-runner from 2.13.1 to 2.13.2
  * Bump docker/setup-qemu-action from 3.6.0 to 3.7.0

## 4.9.1
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Remove Content-Type expectation from PutBucketVersioning (Fixes #2635)
  * Let S3Mock validate bucket names according to AWS rules
* Refactorings
  * Let TaggingHeaderConverter convert XML tags
  * Let Spring convert StorageClass in postObject
  * Fix build errors: skip JavaDoc generation for POM type modules.
  * Build robustness: execute unit and integration tests in parallel and in random order.
  * Faster startup time through lazy initialization
  * Build: move "checkstyle.xml" to "etc/". The "build-config" module was never necessary.
  * Build: update Google Checkstyle to the latest version and fix violations.
  * Build: use ktlint-maven-plugin to validate Kotlin code style.
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.5.5 to 3.5.6
  * Bump aws-v2.version from 2.32.31 to 2.33.12
  * Bump aws.version from 1.12.788 to 1.12.791
* Version updates (build dependencies)
  * Bump Java 21 to Java 25 to build S3Mock.
  * Bump kotlin.version from 2.2.10 to 2.2.20
  * Bump aws.sdk.kotlin:s3-jvm from 1.5.26 to 1.5.41
  * Bump org.xmlunit:xmlunit-assertj3 from 2.10.3 to 2.10.4
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.5.3 to 3.5.4
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.5.3 to 3.5.4
  * Bump com.puppycrawl.tools:checkstyle from 11.0.0 to 11.0.1
  * Bump digital.pragmatech.testing:spring-test-profiler from 0.0.11 to 0.0.12
  * Bump actions/stale from 9.1.0 to 10.0.0
  * Bump github/codeql-action from 3.29.11 to 3.30.3
  * Bump step-security/harden-runner from 2.13.0 to 2.13.1
  * Bump maven from 3.9.9 to 3.9.11
  * Bump maven wrapper from 3.3.2 to 3.3.3

## 4.9.0

Release of Java modules failed, please use 4.9.1

## 4.8.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * CompleteMultipartUpload is idempotent (fixes #2586)
* Refactorings
  * UploadId is always a UUID. Use UUID type in S3Mock instead of String.
  * Validate that partNumbers to be positive integers.
  * Force convergence on the newest available transitive dependency versions.
  * Optimize file storage for large objects by using buffered streams.
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.5.4 to 3.5.5
  * Bump aws-v2.version from 2.32.7 to 2.32.31
  * Bump org.apache.commons:commons-compress from 1.27.1 to 1.28.0
* Version updates (build dependencies)
  * Bump kotlin.version from 2.2.0 to 2.2.10
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.125 to 1.5.26
  * Bump digital.pragmatech.testing:spring-test-profiler from 0.0.5 to 0.0.11
  * Bump com.puppycrawl.tools:checkstyle from 10.26.1 to 11.0.0
  * Bump github/codeql-action from 3.29.4 to 3.29.11
  * Bump actions/checkout from 4.2.2 to 5.0.0
  * Bump actions/setup-java from 4.7.1 to 5.0.0
  * Bump actions/dependency-review-action from 4.7.2 to 4.7.3

## 4.7.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fix store property overrides for "com.adobe.testing.s3mock.store" properties. (Fixes #2524)
* Refactorings
  * Enable Spring Boot Actuator in "debug" and "trace" profiles.
  * Enable [Spring Test Profiler](https://github.com/PragmaTech-GmbH/spring-test-profiler) during test runs.
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.5.3 to 3.5.4
  * Bump aws-v2.version from 2.31.77 to 2.32.7
  * Bump aws.version from 1.12.787 to 1.12.788
  * Bump commons-io:commons-io from 2.19.0 to 2.20.0
  * Bump alpine from 3.22.0 to 3.22.1 in /docker
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.109 to 1.4.125
  * Bump org.apache.maven.plugins:maven-enforcer-plugin from 3.6.0 to 3.6.1
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.11.2 to 3.11.3
  * Bump org.mockito.kotlin:mockito-kotlin from 5.4.0 to 6.0.0
  * Bump step-security/harden-runner from 2.12.2 to 2.13.0
  * Bump github/codeql-action from 3.29.2 to 3.29.10
  * Bump actions/dependency-review-action from 4.7.1 to 4.7.2

## 4.6.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fail PUT object with match on non-existent keys (fixes #2502)
* Refactorings
  * Remove unused imports
  * Fix Kotlin 2.2 usage
  * Ignore .vscode and .cursor configurations
  * Minor refactorings for clarity.
  * Use fixed list of StorageClass values in tests. New values added by AWS sometimes break tests. We want to make sure to test a few different storage classes, no need to test every one.
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.31.67 to 2.31.77
  * Bump testcontainers.version from 1.21.2 to 1.21.3
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.109 to 1.4.119
  * Bump org.apache.maven.plugins:maven-gpg-plugin from 3.2.7 to 3.2.8
  * Bump org.apache.maven.plugins:maven-enforcer-plugin from 3.5.0 to 3.6.0
  * Bump com.puppycrawl.tools:checkstyle from 10.26.0 to 10.26.1
  * Bump github/codeql-action from 3.29.1 to 3.29.2
  * Bump step-security/harden-runner from 2.12.1 to 2.12.2

## 4.5.1
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * ListObjectVersions API returns "isLatest=true" if versioning is not enabled. (fixes #2481)
  * Tags are now verified for correctness.
* Refactorings
  * README.md fixes, typos, wording, clarifications
* Version updates (deliverable dependencies)
  * None
* Version updates (build dependencies)
  * Bump kotlin.version from 2.1.21 to 2.2.0
  * Bump github/codeql-action from 3.29.0 to 3.29.1
  * Bump com.puppycrawl.tools:checkstyle from 10.25.0 to 10.26.0

## 4.5.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Implement DeleteObjectTagging API
* Refactorings
  * Add JSpecify annotations to S3Mock code
  * Migrate unit tests in "testsupport" modules to Kotlin
  * Deprecation of legacy-style Spring properties in favor of current environment variables.
  * Various fixes and clarifications in README.md
* Version updates (deliverable dependencies)
  * Bump alpine from 3.21.3 to 3.22.0 in /docker
  * Bump aws-v2.version from 2.31.50 to 2.31.67
  * Bump aws.version from 1.12.783 to 1.12.787
  * Bump spring-boot.version from 3.5.0 to 3.5.3
  * Bump testcontainers.version from 1.21.0 to 1.21.2
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.91 to 1.4.109
  * Bump org.xmlunit:xmlunit-assertj3 from 2.10.2 to 2.10.3
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.5.0 to 3.5.1
  * Bump org.apache.maven.plugins:maven-clean-plugin from 3.4.1 to 3.5.0
  * Bump com.puppycrawl.tools:checkstyle from 10.24.0 to 10.25.0
  * Bump maven from 3.9.6 to 3.9.9
  * Bump maven-wrapper from 3.2.0 to 3.3.2
  * Bump ossf/scorecard-action from 2.4.1 to 2.4.2
  * Bump github/codeql-action from 3.28.18 to 3.29.0
  * Bump step-security/harden-runner from 2.12.0 to 2.12.1

## 4.4.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fix order of ListObjectVersions (fixes #2412)
* Refactorings
  * Remove configuration exclusions for Spring Security classes
    * We don't include Spring Security dependencies anymore.
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.4.5 to 3.5.0
  * Bump aws-v2.version from 2.31.42 to 2.31.50
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.83 to 1.4.91
  * Bump org.xmlunit:xmlunit-assertj3 from 2.10.0 to 2.10.2
  * Bump com.puppycrawl.tools:checkstyle from 10.23.1 to 10.24.0
  * Bump github/codeql-action from 3.28.17 to 3.28.18

## 4.3.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * S3Mock accepts * for conditional requests on all APIs. (fixes #2371)
  * Clarifications for S3Mock with custom SSL certificate usage in README.md
  * Clarifications for S3Mock with provided SSL certificate usage in README.md
* Refactorings
  * none
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.31.38 to 2.31.42
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.80 to 1.4.83
  * Bump kotlin.version from 2.1.20 to 2.1.21
  * Bump actions/dependency-review-action from 4.7.0 to 4.7.1

## 4.2.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support checksum algorithm CRC64NVME (fixes #2334)
* Refactorings
  * API / DTO consistency check 2025/04
    * Check AWS API for changes
      * Update S3Mock API / DTOs
      * Add tests for changed API / DTOs
    * CreateBucket API now accepts "CreateBucketConfiguration" request body
    * HeadBucket API now returns region and location headers
    * CompleteMultipartUpload API now accepts checksums and returns checksums
    * ListObjects API now returns "delimiter"
    * ListObjects V2 API now accepts "fetch-owner" and returns "delimiter"
    * ListBuckets API now accepts parameters listed in AWS S3 API
    * ListMultipartUploads now accepts parameters listed in AWS S3 API
    * ListParts now accepts parameters listed in AWS S3 API
    * UploadPartCopy now accepts and returns encryption headers
    * CreateMultipartUpload now accepts checksum headers and returns checksum and encryption headers
    * CompleteMultipartUpload now accepts checksum headers and returns checksum and encryption headers
      * Checksum validation on complete
    * DeleteObject now supports conditional requests
    * PutObject now supports conditional requests
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.31.25 to 2.31.38
  * Bump aws.version from 1.12.782 to 1.12.783
  * Bump spring-boot.version from 3.4.4 to 3.4.5
  * Bump testcontainers.version from 1.20.6 to 1.21.0
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.67 to 1.4.80
  * Bump actions/dependency-review-action from 4.6.0 to 4.7.0
  * Bump github/codeql-action from 3.28.15 to 3.28.17
  * Bump com.puppycrawl.tools:checkstyle from 10.23.0 to 10.23.1

## 4.1.1
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Content-Encoding: aws-chunked should not be stored (fixes #2218)
* Refactorings
  * none
* Version updates (deliverable dependencies)
  * none
* Version updates (build dependencies)
  * none

## 4.1.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support Browser-Based Uploads Using POST (fixes #2200)
    * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-UsingHTTPPOST.html
* Refactorings
  * Validate all integration tests against S3, fix S3Mock where necessary
    * These were corner cases where error messages were incorrect, or proper validations were missing.
  * Migrate all integration tests to AWS SDK v2, remove AWS SDK v1 tests from the integration-tests module
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.31.17 to 2.31.25
  * Bump commons-io:commons-io from 2.18.0 to 2.19.0
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.59 to 1.4.67
  * Bump step-security/harden-runner from 2.11.1 to 2.12.0
  * Bump actions/setup-java from 4.7.0 to 4.7.1

## 4.0.0
Version 4.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Basic support for versions in API (fixes #64)
  * Allow overriding headers in head object
  * Implement If-(Un)modified-Since handling (fixes #829)
  * Close all InputStreams and OutputStreams
  * Checksums are returned for MultipartUploads as part of the response body
  * Add AWS SDK V1 deprecation notice
    * AWS has deprecated SDK for Java v1, and will remove support EOY 2025.
    * S3Mock will remove usage of Java v1 early 2026.
* Refactorings
  * Use Tomcat instead of Jetty as the application container (fixes #2136)
  * "FROM" in Dockerfile did not match "as"
  * Delete files on shutdown using a `DisposableBean` instead of `File#deleteOnExit()`
* Version updates (deliverable dependencies)
  * Bump spring-boot.version from 3.3.3 to 3.4.4
  * Jackson 2.18.2 to 2.17.2 (remove override, use Spring-Boot supplied version)
  * Bump aws-v2.version from 2.29.29 to 2.31.17
  * Bump aws.version from 1.12.779 to 1.12.780
  * Bump kotlin.version from 2.1.0 to 2.1.20
  * Bump testcontainers.version from 1.20.4 to 1.20.6
  * Bump org.testng:testng from 7.10.2 to 7.11.0
  * Bump aws.version from 1.12.780 to 1.12.782
  * Bump alpine from 3.21.0 to 3.21.3 in /docker
* Version updates (build dependencies)
  * Bump aws.sdk.kotlin:s3-jvm from 1.4.41 to 1.4.59
  * Bump org.apache.maven.plugins:maven-compiler-plugin from 3.13.0 to 3.14.0
  * Bump org.apache.maven.plugins:maven-clean-plugin from 3.4.0 to 3.4.1
  * Bump org.apache.maven.plugins:maven-install-plugin from 3.1.3 to 3.1.4
  * Bump org.apache.maven.plugins:maven-deploy-plugin from 3.1.3 to 3.1.4
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.5.2 to 3.5.3
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.5.2 to 3.5.3
  * Bump io.fabric8:docker-maven-plugin from 0.45.1 to 0.46.0
  * Bump org.jacoco:jacoco-maven-plugin from 0.8.12 to 0.8.13
  * Bump license-maven-plugin-git.version from 4.6 to 5.0.0
  * Bump github/codeql-action from 3.27.6 to 3.28.15
  * Bump docker/setup-qemu-action from 3.2.0 to 3.6.0
  * Bump actions/upload-artifact from 4.4.3 to 4.6.2
  * Bump actions/setup-java from 4.5.0 to 4.7.0
  * Bump actions/dependency-review-action from 4.5.0 to 4.6.0
  * Bump step-security/harden-runner from 2.10.2 to 2.11.1
  * Bump ossf/scorecard-action from 2.4.0 to 2.4.1
  * Bump com.puppycrawl.tools:checkstyle from 10.20.2 to 10.23.0
  * Bump advanced-security/sbom-generator-action from 0.0.1 to 0.0.2


# DEPRECATED - 3.x
Version 3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**3.x is DEPRECATED and may receive bug fixes and features. This will be best-effort only.**

## 3.12.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * none
* Refactorings
  * none
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.28.11 to 2.29.29
  * Bump aws.version from 1.12.772 to 1.12.779
  * Bump kotlin.version from 2.0.20 to 2.1.0
  * Bump com.fasterxml.jackson:jackson-bom from 2.18.0 to 2.18.2
  * Bump commons-io:commons-io from 2.17.0 to 2.18.0
  * Bump testcontainers.version from 1.20.1 to 1.20.4
  * Bump alpine from 3.20.3 to 3.21.0 in /docker
* Version updates (build dependencies)
  * Bump io.fabric8:docker-maven-plugin from 0.45.0 to 0.45.1
  * Bump com.puppycrawl.tools:checkstyle from 10.18.1 to 10.20.2
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.10.0 to 3.11.2
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.5.0 to 3.5.2
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.5.0 to 3.5.2
  * Bump org.apache.maven.plugins:maven-dependency-plugin from 3.8.0 to 3.8.1
  * Bump org.apache.maven.plugins:maven-checkstyle-plugin from 3.5.0 to 3.6.0
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.4.1 to 3.5.0
  * Bump actions/dependency-review-action from 4.3.4 to 4.5.0
  * Bump actions/setup-java from 4.4.0 to 4.5.0
  * Bump actions/upload-artifact from 3.1.0 to 4.4.3
  * Bump actions/checkout from 4.2.0 to 4.2.2
  * Bump github/codeql-action from 3.26.9 to 3.27.6
  * Bump advanced-security/maven-dependency-submission-action from 3.0.3 to 4.1.1
  * Bump step-security/harden-runner from 2.10.1 to 2.10.2

## 3.11.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * none
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.26.25 to 2.28.11
  * Bump aws.version from 1.12.765 to 1.12.772
  * Bump spring-boot.version from 3.3.2 to 3.3.3
  * Bump commons-io:commons-io from 2.16.1 to 2.17.0
  * Bump com.fasterxml.jackson:jackson-bom from 2.17.2 to 2.18.0
  * Bump testcontainers.version from 1.20.0 to 1.20.1
  * Bump alpine from 3.20.2 to 3.20.3 in /docker
  * Bump kotlin.version from 2.0.0 to 2.0.20

## 3.10.3
Test release that incremented the patch version number.
Please refer / update to version 3.11.0, thanks.

## 3.10.2
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  *  Let CopyObject overwrite store headers (fixes #2005)
* Version updates (build dependencies)
  * Bump org.apache.maven.plugins:maven-deploy-plugin from 3.1.2 to 3.1.3
  * Bump org.apache.maven.plugins:maven-dependency-plugin from 3.7.1 to 3.8.0
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.8.0 to 3.10.0
  * Bump org.apache.maven.plugins:maven-gpg-plugin from 3.2.6 to 3.2.7
  * Bump org.apache.maven.plugins:maven-checkstyle-plugin from 3.4.0 to 3.5.0
  * Bump license-maven-plugin-git.version from 4.5 to 4.6
  * Bump com.puppycrawl.tools:checkstyle from 10.17.0 to 10.18.1
  * Bump actions/checkout from 4.1.7 to 4.2.0
  * Bump github/codeql-action from 3.26.7 to 3.26.9
  * Bump actions/setup-java from 4.3.0 to 4.4.0

## 3.10.1
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  *  CRT-based S3 client has timeouts with mock when uploading streams of unknown size (fixes #2049)
* Version updates (build dependencies)
  * Bump io.fabric8:docker-maven-plugin from 0.44.0 to 0.45.0
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.3.0 to 3.4.1
  * Bump org.apache.maven.plugins:maven-install-plugin from 3.1.2 to 3.1.3
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.3.1 to 3.5.0
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.3.1 to 3.5.0
  * Bump github/codeql-action from 3.25.15 to 3.26.7
  * Bump step-security/harden-runner from 2.9.0 to 2.10.1
  * Bump actions/setup-java from 4.2.1 to 4.3.0
  * Bump actions/upload-artifact from 4.3.4 to 4.4.0

## 3.10.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Allow PUT requests without content-type application/xml (fixes #1978) 
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.25.59 to 2.26.25
  * Bump aws.version from 1.12.729 to 1.12.765
  * Bump spring-boot.version from 3.3.0 to 3.3.2
  * Bump alpine from 3.20.0 to 3.20.2 in /docker
  * Bump com.fasterxml.jackson:jackson-bom from 2.17.1 to 2.17.2
  * Bump testcontainers.version from 1.19.8 to 1.20.0
  * Bump org.mockito.kotlin:mockito-kotlin from 5.3.1 to 5.4.0
* Version updates (build dependencies)
  * Bump com.puppycrawl.tools:checkstyle from 10.16.0 to 10.17.0
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.2.5 to 3.3.1
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.2.5 to 3.3.1
  * Bump org.apache.maven.plugins:maven-enforcer-plugin from 3.4.1 to 3.5.0
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.6.3 to 3.8.0
  * Bump org.apache.maven.plugins:maven-dependency-plugin from 3.6.1 to 3.7.1
  * Bump org.apache.maven.plugins:maven-checkstyle-plugin from 3.3.1 to 3.4.0
  * Bump org.apache.maven.plugins:maven-release-plugin from 3.0.1 to 3.1.1
  * Bump org.apache.maven.plugins:maven-clean-plugin from 3.3.2 to 3.4.0
  * Bump org.apache.maven.plugins:maven-jar-plugin from 3.4.1 to 3.4.2
  * Bump org.sonatype.plugins:nexus-staging-maven-plugin from 1.6.13 to 1.7.0
  * Bump docker/setup-qemu-action from 3.0.0 to 3.2.0
  * Bump actions/upload-artifact from 4.3.3 to 4.3.4
  * Bump actions/dependency-review-action from 4.3.2 to 4.3.4
  * Bump actions/checkout from 4.1.6 to 4.1.7
  * Bump github/codeql-action from 3.25.6 to 3.25.14
  * Bump ossf/scorecard-action from 2.3.3 to 2.4.0
  * Bump step-security/harden-runner from 2.8.0 to 2.9.0

## 3.9.1
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Persist metadata for parts, validate checksum on multipart completion (fixes #1205)
* Refactorings
  * Migrate Unit tests to Kotlin
  * Run ITs against real S3, fix code or tests in case of errors
    * Fix Checksums for Multiparts
    * Add ObjectOwnership config for Buckets, setting ACLs is not allowed otherwise
    * Fix StorageClass, it's not returned for most APIs if it's "STANDARD"
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.25.49 to 2.25.59
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.720 to 1.12.729
  * Bump kotlin.version from 1.9.24 to 2.0.0
  * Bump alpine from 3.19.1 to 3.20.0 in /docker
* Version updates (build dependencies)
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.2.0 to 3.3.0
  * Bump com.github.ekryd.sortpom:sortpom-maven-plugin from 3.4.1 to 4.0.0
  * Bump license-maven-plugin-git.version from 4.4 to 4.5
  * Bump actions/checkout from 4.1.5 to 4.1.6
  * Bump github/codeql-action from 3.25.4 to 3.25.6
  * Bump step-security/harden-runner from 2.7.1 to 2.8.0

## 3.9.0

Release of Java modules failed, please use 3.9.1

## 3.8.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fix failing uploads after EOL detection refactoring (fixes #1840, #1842)
    * Adding additional tests for combinations of HTTP / HTTPS and sync / async clients with different test files
    * Known issue: using HTTP, AWS SDKv2 sends the wrong checksum for SHA256, leading uploads to fail
* Version updates (deliverable dependencies)
  * Bump aws-v2.version from 2.25.39 to 2.25.49
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.709 to 1.12.720
  * Bump com.fasterxml.jackson:jackson-bom from 2.17.0 to 2.17.1
  * Bump kotlin.version from 1.9.23 to 1.9.24
  * Bump org.xmlunit:xmlunit-assertj3 from 2.9.1 to 2.10.0
  * Bump testcontainers.version from 1.19.7 to 1.19.8
  * Bump org.testng:testng from 7.10.1 to 7.10.2
  * Bump com.puppycrawl.tools:checkstyle from 10.15.0 to 10.16.0
* Version updates (build dependencies)
  * Bump license-maven-plugin-git.version from 4.3 to 4.4
  * Bump org.apache.maven.plugins:maven-deploy-plugin from 3.1.1 to 3.1.2
  * Bump org.apache.maven.plugins:maven-install-plugin from 3.1.1 to 3.1.2
  * Bump step-security/harden-runner from 2.7.0 to 2.7.1
  * Bump actions/checkout from 4.1.4 to 4.1.5
  * Bump actions/dependency-review-action from 4.2.5 to 4.3.2
  * Bump ossf/scorecard-action from 2.3.1 to 2.3.3
  * Bump github/codeql-action from 3.25.3 to 3.25.4

## 3.7.3
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support large, chunked, unsigned, asynchronous uploads (fixes #1818)

## 3.7.2
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Calculate and validate checksums on upload (fixes #1827)
    * UploadPart API now also returns checksums, if available.

## 3.7.1
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Correctly handle chunked unsigned uploads (fixes #1662)
    * Known issue: when using one of the Async SDK clients, uploads sometimes fail when the file size is >16KB.
      Not yet sure why. Uploads <16KB work just fine.
  * Let Jetty handle "UNSAFE" characters in URIs, again (see #1686)
* Version updates
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.698 to 1.12.709
  * Bump aws-v2.version from 2.25.28 to 2.25.39
  * Bump spring-boot.version from 3.2.4 to 3.2.5
  * Bump org.apache.maven.plugins:maven-gpg-plugin from 3.2.2 to 3.2.4
  * Bump org.apache.maven.plugins:maven-jar-plugin from 3.3.0 to 3.4.1
  * Bump github/codeql-action from 3.24.10 to 3.25.3
  * Bump actions/upload-artifact from 4.3.1 to 4.3.3
  * Bump actions/checkout from 4.1.2 to 4.1.4

## 3.7.0

Release of Java modules failed, please use 3.7.1

## 3.6.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Return XML prolog and namespace in all responses (fixes #1754)
  * Explain in README.md how `root` property works with Docker. (fixes #1728)
* Refactorings
  * Removal of JAX-B for AccessControlPolicy requests/responses.
    * Jackson-databind-xml 2.17.0 adds polymorphic (de-)serializiation through "xsi:type"
  * Jackson-annotation cleanup in POJOs
* Version updates
  * Bump spring-boot.version from 3.2.3 to 3.2.4
  * Bump aws-v2.version from 2.24.9 to 2.25.28
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.665 to 1.12.698
  * Bump commons-io:commons-io from 2.15.1 to 2.16.1
  * Bump org.testng:testng from 7.9.0 to 7.10.1
  * Bump org.mockito.kotlin:mockito-kotlin from 5.2.1 to 5.3.1
  * Bump com.puppycrawl.tools:checkstyle from 10.14.0 to 10.15.0
  * Bump org.apache.maven.plugins:maven-gpg-plugin from 3.1.0 to 3.2.2
  * Bump org.apache.maven.plugins:maven-compiler-plugin from 3.12.1 to 3.13.0
  * Bump org.apache.maven.plugins:maven-source-plugin from 3.3.0 to 3.3.1
  * Bump com.github.ekryd.sortpom:sortpom-maven-plugin from 3.4.0 to 3.4.1
  * Bump org.jacoco:jacoco-maven-plugin from 0.8.11 to 0.8.12
  * Bump actions/checkout from 4.1.1 to 4.1.2
  * Bump actions/setup-java from 4.1.0 to 4.2.1
  * Bump github/codeql-action from 3.24.6 to 3.24.10
  * Bump actions/dependency-review-action from 4.1.3 to 4.2.5
  * Bump maven from 3.8.5 to 3.9.6

## 3.5.2
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support key names that lead to request paths that do not comply to RFC3986 (fixes #1686)
* Refactorings
  * Refactor IT usage of S3 clients, add more tests
  * Use ZGC and ZGenerationalGC when running in Docker
* Version updates
  * Bump kotlin.version from 1.9.22 to 1.9.23
  * Bump testcontainers.version from 1.19.6 to 1.19.7
  * Bump github/codeql-action from 3.24.5 to 3.24.6
  * Bump actions/setup-java from 4.0.0 to 4.1.0
  * Bump com.puppycrawl.tools:checkstyle from 10.13.0 to 10.14.0

## 3.5.1
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support canned ACLs (fixes #1617)
* Version updates
  * Bump spring-boot.version from 3.2.2 to 3.2.3
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.656 to 1.12.665
  * Bump aws-v2.version from 2.23.21 to 2.24.9
  * Bump testcontainers.version from 1.19.5 to 1.19.6
  * Bump io.fabric8:docker-maven-plugin from 0.43.4 to 0.44.0
  * Bump com.github.ekryd.sortpom:sortpom-maven-plugin from 3.3.0 to 3.4.0
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.1.1 to 3.2.0
  * Bump actions/dependency-review-action from 4.0.0 to 4.1.3
  * Bump github/codeql-action from 3.24.0 to 3.24.5

## 3.5.0

Release of Java modules failed, please use 3.5.1

## 3.4.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support storage classes in APIs
  * Use "application/octet-stream" as default content-type
* Refactorings
  * Use JDK21 as runtime in the Docker container
* Version updates
  * Bump spring-boot.version from 3.2.1 to 3.2.2
  * Bump aws-v2.version from 2.22.7 to 2.23.21
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.627 to 1.12.656
  * Bump testcontainers.version from 1.19.3 to 1.19.5
  * Bump com.puppycrawl.tools:checkstyle from 10.12.6 to 10.13.0
  * Bump alpine from 3.19.0 to 3.19.1 in /docker
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.2.3 to 3.2.5
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.2.3 to 3.2.5
  * Bump github/codeql-action from 3.22.12 to 3.24.0
  * Bump actions/upload-artifact from 4.0.0 to 4.3.1
  * Bump actions/dependency-review-action from 3.1.4 to 4.0.0
  * Bump step-security/harden-runner from 2.6.1 to 2.7.0

## 3.3.1

Accidental release, should have been 3.4.0

## 3.3.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support initial and existing buckets (fixes #1433)
  * Compile with "parameters=true" (fixes #1555)
* Version updates
  * Bump spring-boot.version from 3.1.5 to 3.2.1
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.580 to 1.12.627
  * Bump aws-v2.version from 2.21.14 to 2.22.7
  * Bump commons-io:commons-io from 2.15.0 to 2.15.1
  * Bump testcontainers.version from 1.19.1 to 1.19.3
  * Bump kotlin.version from 1.9.20 to 1.9.22
  * Bump alpine from 3.18.4 to 3.19.0 in /docker
  * Bump org.testng:testng from 7.8.0 to 7.9.0
  * Bump org.mockito.kotlin:mockito-kotlin from 5.1.0 to 5.2.1
  * Bump com.puppycrawl.tools:checkstyle from 10.12.4 to 10.12.6
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.2.1 to 3.2.3
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.2.1 to 3.2.3
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.6.0 to 3.6.3
  * Bump org.apache.maven.plugins:maven-compiler-plugin from 3.11.0 to 3.12.1
  * Bump org.codehaus.mojo:exec-maven-plugin from 3.1.0 to 3.1.1
  * Bump actions/setup-java from 3.13.0 to 4.0.0
  * Bump step-security/harden-runner from 2.6.0 to 2.6.1
  * Bump actions/dependency-review-action from 3.1.0 to 3.1.4
  * Bump actions/upload-artifact from 3.1.3 to 4.0.0
  * Bump github/codeql-action from 2.22.5 to 3.22.12
  * Bump mvn version from 3.5.4 to 3.8.5

## 3.2.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Better description of S3Mock API usage and SDK usage (fixes #219, #125, #1196)
    * Presigned URLs were working all along. 
    * Added documentation and tests.
  * Add ListObjectVersions API - dummy implementation (fixes #1215) 
* Version updates
  * Bump aws-v2.version from 2.20.115 to 2.21.14
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.519 to 1.12.580
  * Bump spring-boot.version from 3.1.0 to 3.1.5
  * Bump alpine from 3.18.2 to 3.18.4 in /docker
  * Bump testcontainers.version from 1.18.3 to 1.19.1
  * Bump kotlin.version from 1.9.0 to 1.9.20
  * Bump commons-io:commons-io from 2.13.0 to 2.15.0
  * Bump com.puppycrawl.tools:checkstyle from 10.12.2 to 10.12.4
  * Bump io.fabric8:docker-maven-plugin from 0.43.2 to 0.43.4
  * Bump org.apache.maven.plugins:maven-enforcer-plugin from 3.3.0 to 3.4.1
  * Bump org.apache.maven.plugins:maven-javadoc-plugin from 3.5.0 to 3.6.0
  * Bump org.apache.maven.plugins:maven-dependency-plugin from 3.6.0 to 3.6.1
  * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.1.2 to 3.2.1
  * Bump org.apache.maven.plugins:maven-failsafe-plugin from 3.1.2 to 3.2.1
  * Bump org.apache.maven.plugins:maven-checkstyle-plugin from 3.3.0 to 3.3.1
  * Bump org.apache.maven.plugins:maven-clean-plugin from 3.3.1 to 3.3.2
  * Bump license-maven-plugin-git.version from 4.2 to 4.3
  * Bump org.jacoco:jacoco-maven-plugin from 0.8.10 to 0.8.11
  * Bump org.mockito.kotlin:mockito-kotlin from 5.0.0 to 5.1.0
  * Bump actions/setup-java from 3.12.0 to 3.13.0
  * Bump actions/checkout from 3.5.3 to 4.1.1
  * Bump actions/upload-artifact from 3.1.2 to 3.1.3
  * Bump actions/dependency-review-action from 3.0.6 to 3.1.0
  * Bump github/codeql-action from 2.21.2 to 2.22.5
  * Bump docker/setup-qemu-action from 2.2.0 to 3.0.0
  * Bump step-security/harden-runner from 2.5.0 to 2.6.0
  * Bump ossf/scorecard-action from 2.2.0 to 2.3.1

## 3.1.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fixes various bugs, vulnerabilities, code smells, security hot spots etc found by Sonarqube
* Refactorings
  * Scanned repo with StepSecurity tools
    * Merged various PRs related to the security of Github actions and Github review actions
    * Reviewed [OSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/adobe/S3Mock), applied various fixes
    * Reviewed [OSSF Best practices](https://bestpractices.coreinfrastructure.org/projects/7673)
* Version updates
  * Bump com.amazonaws:aws-java-sdk-s3 from 1.12.501 to 1.12.519
  * Bump aws-v2.version from 2.20.98 to 2.20.115
  * Bump com.github.ekryd.sortpom:sortpom-maven-plugin from 3.2.1 to 3.3.0
  * Bump io.fabric8:docker-maven-plugin from 0.43.0 to 0.43.2
  * Bump com.puppycrawl.tools:checkstyle from 10.12.1 to 10.12.2
  * Bump kotlin.version from 1.8.22 to 1.9.0
  * Bump github/codeql-action from 2.21.1 to 2.21.2

## 3.0.1
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Fix startup on existing data folder issues (fixes #1245)
  * Return checksumAlgorithm in ListObjects / ListObjectsV2 (fixes #1220)

## 3.0.0
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Refactorings
  * Use various Java language features introduced between JDK 8 and JDK 17
  * Use new Spring Boot 3 features
  * Use new Spring Framework 6 features
  * Dependency cleanup
  * Code cleanup
  * Deprecate JUnit 4 TestRule
    * This module will be removed in S3Mock 4.x
* Version updates
  * Bump spring-boot.version from 2.7.13 to 3.1.0
    * This updates all Spring Boot dependencies as well
  * Bump Spring Framework to 6.0.9
  * Bump Java bytecode version from 8 to 17
    * This change is necessary, as Spring Framework 6 and Spring Boot 3 raise the baseline Java version from 8 to 17.

# DEPRECATED - 2.x
Version 2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**2.x is DEPRECATED and may receive bug fixes and features, this will be best-effort only.**

## 2.17.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Add support for ChecksumAlgorithm (fixes #1123)
    * Support checksumAlgorithm where AWS SDK locally calculates the checksum and sends it as part of the request body.
    * Support checksum headers where clients can send an already calculated checksum for the backend to persist
    * Return checksum in getObject / putObject / headObject / getObjectAttributes
  * Consistent consumes / produces declarations (fixes #1208)
* Version updates
  * Bump aws-v2.version from 2.20.96 to 2.20.98
  * Bump aws-java-sdk-s3 from 1.12.499 to 1.12.501

## 2.16.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Add support for GetObjectAttributes API (fixes #536)
* Version updates
  * Bump aws-v2.version from 2.20.94 to 2.20.96
  * Bump aws-java-sdk-s3 from 1.12.497 to 1.12.499
  * Bump maven-wrapper from 3.1.1 to 3.2.0

## 2.15.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Store and return all encryption headers (fixes #1178)
* Version updates
  * Bump aws-v2.version from 2.20.92 to 2.20.94
  * Bump aws-java-sdk-s3 from 1.12.494 to 1.12.497
  * Bump checkstyle from 10.12.0 to 10.12.1

## 2.15.0
Release that incremented the version number but was not successfully released to both Maven One and Docker Hub.
Please refer / update to version 2.15.1, thanks.

## 2.14.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Storage and retrieval headers like Content-Disposition (fixes #1163)
* Refactorings
  * Test that persisted file length is equal to uploaded file length
    * This does not work if checksum validation is enabled (see #1123)
* Version updates
  * Bump spring-boot.version from 2.7.12 to 2.7.13
  * Bump aws-v2.version from 2.20.86 to 2.20.92
  * Bump aws-java-sdk-s3 from 1.12.488 to 1.12.494
  * Bump maven-clean-plugin from 3.2.0 to 3.3.1

## 2.13.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Correct Range header handling with Spring's HttpRange (fixes #1174)

## 2.13.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Accept X-Amz-Meta headers regardless of case (fixes #1160)
* Refactorings
  * Add junit-jupiter dependency to s3mock-testcontainers module
    * This was necessary because testcontainers 1.18.0 removed transitive jupiter dependencies.
* Version updates
  * Bump spring-boot.version from 2.7.7 to 2.7.12
  * Bump commons-io from 2.11.0 to 2.13.0
  * Bump testcontainers.version from 1.17.6 to 1.18.3
  * Bump aws-java-sdk-s3 from 1.12.389 to 1.12.488
  * Bump aws-v2.version from 2.19.16 to 2.20.86
  * Bump alpine from 3.17.1 to 3.18.2 in /docker
  * Bump kotlin.version from 1.7.22 to 1.8.22
  * Bump docker-maven-plugin from 0.40.3 to 0.43.0
  * Bump testng from 7.7.1 to 7.8.0
  * Bump checkstyle from 10.6.0 to 10.12.0
  * Bump maven-enforcer-plugin from 3.1.0 to 3.3.0
  * Bump maven-dependency-plugin from 3.4.0 to 3.6.0
  * Bump maven-deploy-plugin from 3.0.0 to 3.1.1
  * Bump maven-checkstyle-plugin from 3.2.1 to 3.3.0
  * Bump maven-install-plugin from 3.1.0 to 3.1.1
  * Bump maven-javadoc-plugin from 3.4.1 to 3.5.0
  * Bump maven-source-plugin from 3.2.1 to 3.3.0
  * Bump maven-resources-plugin from 3.3.0 to 3.3.1
  * Bump maven-gpg-plugin from 3.0.1 to 3.1.0
  * Bump maven-surefire-plugin from 3.0.0-M8 to 3.1.2
  * Bump maven-failsafe-plugin from 3.0.0-M8 to 3.1.2
  * Bump maven-compiler-plugin from 3.10.1 to 3.11.0
  * Bump maven-release-plugin from 3.0.0-M7 to 3.0.1
  * Bump mockito-kotlin from 4.1.0 to 5.0.0
  * Bump license-maven-plugin-git.version from 4.1 to 4.2

## 2.12.2
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Speed up AwsChunkedDecodingInputStream (fixes #1115)

## 2.12.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Expose all headers for CORS requests (fixes #1023)
  * Fixed various error responses, validations and http return codes
    * Verified all integration tests against the AWS S3 API, fixed S3Mock to match S3 responses exactly.
* Version updates
  * Bump spring-boot.version from 2.7.6 to 2.7.7
  * Bump testng from 7.7.0 to 7.7.1
  * Bump checkstyle from 10.5.0 to 10.6.0
  * Bump alpine from 3.17.0 to 3.17.1 in /docker
  * Bump xmlunit-assertj3 from 2.9.0 to 2.9.1
  * Bump maven-failsafe-plugin from 3.0.0-M7 to 3.0.0-M8
  * Bump maven-checkstyle-plugin from 3.2.0 to 3.2.1
  * Bump maven-surefire-plugin from 3.0.0-M7 to 3.0.0-M8
  * Bump aws-java-sdk-s3 from 1.12.369 to 1.12.389
  * Bump aws-v2.version from 2.19.1 to 2.19.16

## 2.12.0
Test release that incremented the version number but was not released to Maven One or Docker Hub.
Please refer / update to version 2.12.1, thanks.

## 2.11.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support for GetBucketLocation API (fixes #985)
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.346 to 1.12.369
  * Bump aws-v2.version from 2.18.21 to 2.19.1
  * Bump docker-maven-plugin from 0.40.2 to 0.40.3
  * Bump maven-dependency-plugin from 3.3.0 to 3.4.0
  * Bump mockito-kotlin from 4.0.0 to 4.1.0
  * Bump checkstyle from 10.4 to 10.5.0
  * Bump kotlin.version from 1.7.21 to 1.7.22
  * Bump alpine from 3.16.3 to 3.17.0 in /docker
  * Bump spring-boot.version from 2.7.5 to 2.7.6

## 2.10.2
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Let S3Mock return correct errors for invalid bucket names (fixes #935)
    * Previous implementation returned a Spring generated error which does not disclose what's actually wrong
    * If the bucket name is not valid, the bucket can't be created. If a later request still contains this invalid name, S3Mock will now return a 404 not found.

## 2.10.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Let S3Mock use streams for MD5 verification (fixes #939)
    * Previous implementation read the full stream into memory, leading to OutOfMemory errors if the file is larger than the available heap.

## 2.10.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Let S3Mock use container memory and cpu (fixes #922)
    * Set resource limits through docker like this: `docker run -it --memory="1g" --cpus="1.0"`
* Version updates
  * Bump alpine from 3.16.2 to 3.16.3 in /docker
  * Bump testcontainers.version from 1.17.5 to 1.17.6
  * Bump maven-install-plugin from 3.0.1 to 3.1.0
  * Bump aws-v2.version from 2.18.15 to 2.18.21
  * Bump aws-java-sdk-s3 from 1.12.340 to 1.12.346

## 2.9.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * IDs in stores must be different for all objects (fixes #877) 

## 2.9.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support restarting S3Mock with the `retainFilesOnExit` option enabled. (fixes #818, #877) 
  * Let AWS SDKv2 use path style access (fixes #880)
    * Starting with AWS SDKv2.18.x domain style access is the default. This is currently not
      supported by S3Mock.
* Version updates
  * Bump aws-v2.version from 2.17.284 to 2.18.15
  * Bump aws-java-sdk-s3 from 1.12.313 to 1.12.340
  * Bump kotlin.version from 1.7.20 to 1.7.21
  * Bump maven-release-plugin from 3.0.0-M6 to 3.0.0-M7
  * Bump checkstyle from 10.3.4 to 10.4
  * Bump spring-boot.version from 2.7.4 to 2.7.5
  * Bump testcontainers.version from 1.17.4 to 1.17.5

## 2.8.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Add support for BucketLifecycleConfiguration APIs (fixes #291)
    * Implement GetBucketLifecycleConfiguration / PutBucketLifecycleConfiguration / DeleteLifecycleConfiguration
    * S3Mock currently does not enforce the lifecycle configuration.
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.309 to 1.12.312
  * Bump aws-v2.version from 2.17.281 to 2.17.284
  * Bump kotlin.version from 1.7.10 to 1.7.20
  * Bump checkstyle from 10.3.3 to 10.3.4
  * Bump testcontainers.version from 1.17.3 to 1.17.4

## 2.7.2
Accidentally released 2.7.2 instead of 2.8.0 - please ignore :(

## 2.7.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Avoid root folder temporary directory race condition (fixes #837)
    * Thanks to @ginkel for the fix!

## 2.7.0
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Add support for ACL APIs (fixes #213 / #290)
    * Implement GetObjectACL / PutObjectACL
    * Return / accept String instead of a POJO. We need to use JAX-B annotations instead of Jackson annotations
      because AWS decided to use xsi:type annotations in the XML representation, which are not supported
      by Jackson. It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.298 to 1.12.309
  * Bump aws-v2.version from 2.17.269 to 2.17.281
  * Bump spring-boot.version from 2.7.3 to 2.7.4
  * Bump maven-jar-plugin from 3.2.2 to 3.3.0

## 2.6.3
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Handle all incoming and outgoing ETags according to RFC2616 / RFC7232 (fixes #807)
    * Fixes ETag handling in GetObject, HeadObject, CopyObject, UploadPartCopy APIs.
    * We are now generating and storing ETags in the "RFC-Format" with enclosing double quotes.
    * Incoming ETags are used verbatim to compare against the internally held representation.
    * Wildcard ETags are also correctly handled.

## 2.6.2
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * LastModified must be updated when copying an object onto itself (fixes #811)
* Refactorings
  * Split up integration tests by type

## 2.6.1
2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * ETags in requests and responses must comply to RFC-2616 (fixes #801) 
    * Fixes `ListObjects` and `ListObjectsV2` APIs.
    * Whenever S3Mock uses Serialization / Deserialization with Jackson, we  must use our custom 
      EtagSerializer / EtagDeserializer that wraps and unwraps the etag for the DTO.
* Version updates
  * Bump aws-v2.version from 2.17.267 to 2.17.269
  * Bump aws-java-sdk-s3 from 1.12.296 to 1.12.298

## 2.6.0
* Features and fixes
  * Add support for LegalHold APIs (fixes #157)
    * Implement GetObjectLegalHold / PutObjectLegalHold
    * Implement GetObjectLockConfiguration / PutObjectLockConfiguration
    * In S3, object locking can only be activated for versioned buckets, versions are currently not supported by S3Mock.
      S3 enforces LegalHold only for versions, so S3Mock currently can't enforce the legal hold.
  * Add support for Retention APIs
    * Implement GetObjectRetention / PutObjectRetention
    * In S3, object locking can only be activated for versioned buckets, versions are currently not supported by S3Mock.
      S3 enforces retention only for versions, so S3Mock currently can't enforce the retention.
* Refactorings
  * Added support for IntegrationTests to use a dedicated bucket per test method
    * Refactored some IntegrationTests to this pattern.
  * Use AWS SDK internal Utils for instants and encoding
    * Using `software.amazon.awssdk.utils.DateUtils` and `software.amazon.awssdk.utils.http.SdkHttpUtils`
    * These classes are marked as `software.amazon.awssdk.annotations.SdkProtectedApi`
      which means that SDK users (liek S3Mock) should not use these classes, but S3Mock has to exactly match the
      behaviour and expectations of the AWS SDKs, and it's easier to depend on internal AWS SDK classes
      instead of replicating or copying the code.
    * S3Mock may have violated the AWS SDK license before by including source code without explicitly
      stating that it's AWS copyrighted code. (In the JavaDoc it did say that this is a copy with
      a reference to the class, though)
* Version updates
  * Bump aws-v2.version from 2.17.263 to 2.17.267
  * Bump aws-java-sdk-s3 from 1.12.292 to 1.12.296

## 2.5.4
* Features and fixes
  * Rename local storage files, add file system structure documentation to README.md (fixes #220)
  * `@Configuration` / `@ConfigurationProperties` classes now public to enable customers to use `@Import` / `@ContextConfiguration` on them. (fixes #751)

## 2.5.3
* Features and fixes
  * Remove [Spring Component Index](https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#beans-scanning-index) from S3Mock (fixes #786)
    * Adding a Spring Component Index file is a breaking change for all clients of the s3mock.jar
    * If Spring finds even one Component Index file in the classpath, all other configuration in the application 
      is completely ignored by default.

## 2.5.2
* Features and fixes
  * Correctly detect and use existing root folder (fixes #786)

## 2.5.1
* Features and fixes
  * Use `@SpringBootApplication` to configure S3Mock (fixes #782)

## 2.5.0
* Features and fixes
  * Add [Spring Component Index](https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#beans-scanning-index) to S3Mock (fixes #751)
  * Bucket lifecycle
    * S3Mock now validates incoming Bucket names.
      * S3 SDKs (for Java) validated the names before sending, so this should not matter for most use-cases.
    * S3Mock now throws errors on duplicate creation (409 Conflict) / deletion (404 Not Found)
      * These errors are defined in the S3 API, the S3Mock just never implemented them.
  * DTOs
    * Fix names, methods, return values to match AWS API
    * Remove internal field "path" from `Bucket` serializations. Fortunately this did not break AWS SDKs in the past, since the "path" field is not expected in those responses.
  * Various other fixes like
    * Removal of duplicated / simplification of code where possible
    * Add (hopefully useful?) logging with all incoming parameters on errors.
    * Fix IntelliJ IDEA warnings
    * Better assertions in tests
    * Fix various JavaDoc issues, add links to S3 API where possible
* Refactorings
  * Major refactoring towards smaller classes with well-defined responsibilities.
    * Many of the existing lines of code in the S3Mock core were changed, moved or removed.
    * All IntegrationTests were/are still green, HTTP API did not change (other than fixed listed above)
  * Refactor into layers `Controller -> Service -> Store`
    * Controllers handle request/response only, Services implement higher level functionality on top of their stores, Stores read and write data from/to disk.
    * Handle Multipart requests in `MultipartController` -> `MultipartService` -> `MultipartStore`
    * Handle Bucket requests in `BucketController` -> `BucketService` -> `BucketStore`
    * Handle Object requests in `ObjectController` -> `ObjectService` -> `ObjectStore`
    * Code and documentation cleanup
    * Let `BucketStore` store `BucketMetadata` just like `S3ObjectMetadata` locally. For now, only store the "core" metadata like creationDate and name.
    * Store object keys in `BucketMetadata`, assign UUIDs (fixes #94)
    * Store objects in UUID folders, clean up name usage
  * Extract header helper methods into `HeaderUtil` from `FileStoreController` / `ObjectController`.
* Version updates
  * Bump maven-javadoc-plugin from 3.4.0 to 3.4.1
  * Bump aws-v2.version from 2.17.248 to 2.17.263
  * Bump aws-java-sdk-s3 from 1.12.278 to 1.12.292
  * Bump checkstyle from 10.3.2 to 10.3.3
  * Bump maven-checkstyle-plugin from 3.1.2 to 3.2.0
  * Bump spring-boot.version from 2.7.2 to 2.7.3

## 2.4.16
* Features and fixes
  * Fix ListMultiPartUploads for Amazon SDK Java v2 (fixes #734) 
* Refactorings
  * Add [Maven Wrapper](https://maven.apache.org/wrapper) for reliable builds
  * Add Spring Boot "devtools" for better local development
  * Add "debug" profile that logs request output and enables JMX and all actuator endpoints
* Version updates
  * Bump aws-java-sdk-s3 from 1.12.267 to 1.12.278
  * Bump aws-v2.version from 2.17.239 to 2.17.248
  * Bump docker-maven-plugin from 0.40.1 to 0.40.2
  * Bump alpine from 3.16.1 to 3.16.2 in /docker
  * Bump checkstyle from 10.3.1 to 10.3.2

## 2.4.15
Test release that incremented the version number but was not released to Maven One or Docker Hub.
Please refer / update to version 2.4.16, thanks.

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

# DEPRECATED - 1.x

## 1.0.0

Initial Release
