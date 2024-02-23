# Changelog

**The CURRENT major version 3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration. 
3.x will receive new features, dependency updates and bug fixes on a continuous basis.**

**The DEPRECATED major version 2.x is JDK8 LTS bytecode compatible, with Docker and JUnit / direct Java integration.
2.x may receive bug fixes and features, this will be best-effort only.**

<!-- TOC -->
* [Changelog](#changelog)
* [PLANNED - 4.x - RELEASE TBD ~ late 2023 / early 2024](#planned---4x---release-tbd--late-2023--early-2024)
    * [Planned changes](#planned-changes)
* [CURRENT - 3.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT](#current---3x---this-version-is-under-active-development)
  * [3.6.0 - PLANNED](#360---planned)
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
  * [1.0.0](#100)
<!-- TOC -->

# PLANNED - 4.x - RELEASE TBD ~ late 2023 / early 2024
Version 4.x is JDK17 LTS bytecode compatible (maybe JDK21 LTS, depending on the release date), with Docker integration.

Any JUnit / direct Java usage support will most likely be dropped and only supported on a best-effort basis.
(i.e. the modules will be deleted from the code base and not released anymore. It *may* be possible to
still run S3Mock directly in Java.)
The S3Mock is a Spring Boot application and currently contains various workarounds to make it possible
to easily to run `S3MockApplication#start` from a static context. These workarounds will be deleted.

Running S3Mock in unit tests is still supported by using [TestContainers](https://www.testcontainers.org/).

**Once 4.x is released, 3.x may receive bug fixes and features, this will be best-effort only.**

### Planned changes

* Features and fixes
  * TBD
* Refactorings
  * Looking to Remove unit test modules. This enables
    * Refactoring S3Mock to a "standard" Spring Boot application.
    * Removal of workarounds to use `S3MockApplication#start` from a static context
    * Removal of properties workarounds
  * Maybe migration to `Kotlin` - the IntegrationTests were migrated already.
* Version updates
  * Bump Spring Boot version to 4.x
  * Bump Spring Framework version to 7.x
  * Bump java version from 17 to 21 (?)

# CURRENT - 3.x - THIS VERSION IS UNDER ACTIVE DEVELOPMENT
Version 3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

**The current major version 3 will receive new features, dependency updates and bug fixes on a continuous basis.**

## 3.6.0 - PLANNED
3.x is JDK17 LTS bytecode compatible, with Docker and JUnit / direct Java integration.

* Features and fixes
  * Support Versions in APIs
  * Add "DeleteObjectTagging" API
* Refactorings
  * TBD
* Version updates
  * TBD

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

## 1.0.0

Initial Release
