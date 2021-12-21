# Changelog

## 2.3.2

* Set only one CORS header (fixes #74 - again)
* Using official Alpine Docker container with JDK17 from APK to run
  * [alpine:3.15.0](https://hub.docker.com/_/alpine)
  * This is in preparation of multi-arch release
* Add examples for validKmsKeys and initialBuckets configuration (fixes #322)
* Added dependabot, merged various patch and minor version updates:
  * Bump spring-boot from 2.3.12.RELEASE to 2.6.1
  * Bump aws-v2 from 2.17.73 to 2.17.99
  * Bump aws-java-sdk-s3 from 1.12.15 to 1.12.129
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

## 2.2.3

* Set bucket for multipart uploads, return by bucket. (Fixes #292)

## 2.2.2

* Adds option "retainFilesOnExit" to keep files after exit. Default is to remove all files. (Fixes #286)
* Fixes ignored "root" environment variable in Docker (Fixes #289)
* Support CORS headers from any host (fixes #74)

## 2.2.1

* Fixes copy part / copy object encoding issues (Fixes #279)

## 2.2.0

* Add TestContainers support

## 2.1.35

* Delete all files on JVM shutdown (Fixes #249)
* Extract Docker build and integration tests to separate modules
* Docker build and integration test is now optional, run with "-DskipDocker" to skip the Docker build and the integration tests. (Fixes #235)

## 2.1.34

* ETag value now enclosed in quotation marks
* All dates are formatted in UTC timezone (Fixes #203)
* "CommonPrefixes" are now serialized als multiple elements containing one "Prefix" (Fixes #215)
* Removed several superfluous / erroneous elements like "truncated" or "part" from various responses

## 2.1.33

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
* Fixed potential NPE in FileStore

## 2.1.32

* Fixes getS3Object with absolute path (Fixes #245 and #248)

## 2.1.31

* Updated spring-security-oauth2 from 2.3.5.RELEASE to 2.3.6.RELEASE

## 2.1.30

* Fix encoded responses for aws-cli (Fixes #257)
* Updated commons-io from 2.6 to 2.7

## 2.1.29

* Add encodingType as return parameter of ListBucket and ListBucketV2
* Updated junit from 4.13-beta-1 to 4.13.1

## 2.1.28

* Changes to build system, test release

## 2.1.27

* Remove accidental JDK9+ bytecode dependency (Fixes #243)

## 1.0.0

Initial Release
