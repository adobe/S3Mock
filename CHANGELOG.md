# Changelog

## 2.2.2

* Adds option "retainFilesOnExit" to keep files after exit. Default is to remove all files. (Fixes #286)

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
