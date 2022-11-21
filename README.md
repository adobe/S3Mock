[![Latest Version](https://img.shields.io/maven-central/v/com.adobe.testing/s3mock.svg?maxAge=3600&label=Latest%20Release)](https://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.adobe.testing%20a%3As3mock)
[![Docker Hub](https://img.shields.io/badge/docker-latest-blue.svg)](https://hub.docker.com/r/adobe/s3mock/)
![Maven Build](https://github.com/adobe/S3Mock/workflows/Maven%20Build/badge.svg)
[![Java17](https://img.shields.io/badge/MADE%20with-Java17-RED.svg)](#Java)
[![Kotlin](https://img.shields.io/badge/MADE%20with-Kotlin-RED.svg)](#Kotlin)
[![Docker Pulls](https://img.shields.io/docker/pulls/adobe/s3mock)](https://hub.docker.com/r/adobe/s3mock)
[![GitHub stars](https://img.shields.io/github/stars/adobe/S3Mock.svg?style=social&label=Star&maxAge=2592000)](https://github.com/adobe/S3Mock/stargazers/)

<!-- TOC -->
  * [S3Mock](#s3mock)
  * [Changelog](#changelog)
  * [Implemented S3 APIs](#implemented-s3-apis)
  * [File System Structure](#file-system-structure)
    * [Root-Folder](#root-folder)
    * [Buckets](#buckets)
    * [Objects](#objects)
    * [Multipart Uploads](#multipart-uploads)
  * [Usage](#usage)
    * [Configuration](#configuration)
    * [S3Mock Docker](#s3mock-docker)
      * [Start using the command-line](#start-using-the-command-line)
      * [Start using the Fabric8 Docker-Maven-Plugin](#start-using-the-fabric8-docker-maven-plugin)
      * [Start using Testcontainers](#start-using-testcontainers)
    * [S3Mock Java](#s3mock-java)
      * [Start using the JUnit4 Rule](#start-using-the-junit4-rule)
      * [Start using the JUnit5 Extension](#start-using-the-junit5-extension)
      * [Start using the TestNG Listener](#start-using-the-testng-listener)
      * [Start programmatically](#start-programmatically)
  * [Build & Run](#build--run)
    * [Java](#java)
    * [Kotlin](#kotlin)
  * [Contributing](#contributing)
  * [Licensing](#licensing)
<!-- TOC -->

## S3Mock
`S3Mock` is a lightweight server that implements parts of the [Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).  
It has been created to support local integration testing by reducing infrastructure dependencies.

The `S3Mock` server can be started as a standalone *Docker* container, using *Testcontainers*, *JUnit4*, *JUnit5* and *TestNG* support, or programmatically.

## Changelog

See the [changelog](CHANGELOG.md) for detailed information about changes in releases and planned changes.  
We also use GitHub's [releases](https://github.com/adobe/S3Mock/releases) to document changes.

## Implemented S3 APIs

The following [actions are supported by Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html), but not all are implemented by S3Mock.

| API                                                                                                                                                 | Implementation                            |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------|
| [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)                                               | :white_check_mark:                        |
| [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)                                         | :white_check_mark:                        |
| [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)                                                                   | :white_check_mark:                        |
| [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)                                                               | :white_check_mark:                        |
| [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)                                             | :white_check_mark:                        |
| [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)                                                               | :white_check_mark:                        |
| [DeleteBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketAnalyticsConfiguration.html)                   | :x:                                       |
| [DeleteBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html)                                                       | :x:                                       |
| [DeleteBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html)                                           | :x:                                       |
| [DeleteBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketIntelligentTieringConfiguration.html) | :x:                                       |
| [DeleteBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketInventoryConfiguration.html)                   | :x:                                       |
| [DeleteBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html)                                             | :white_check_mark:                        |
| [DeleteBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketMetricsConfiguration.html)                       | :x:                                       |
| [DeleteBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html)                             | :x:                                       |
| [DeleteBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html)                                                   | :x:                                       |
| [DeleteBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketReplication.html)                                         | :x:                                       |
| [DeleteBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html)                                                 | :x:                                       |
| [DeleteBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketWebsite.html)                                                 | :x:                                       |
| [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)                                                               | :white_check_mark:                        |
| [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)                                                             | :white_check_mark:                        |
| [DeleteObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html)                                                 | :x:                                       |
| [DeletePublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletePublicAccessBlock.html)                                         | :x:                                       |
| [GetBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAccelerateConfiguration.html)                       | :x:                                       |
| [GetBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html)                                                               | :x:                                       |
| [GetBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAnalyticsConfiguration.html)                         | :x:                                       |
| [GetBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html)                                                             | :x:                                       |
| [GetBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html)                                                 | :x:                                       |
| [GetBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketIntelligentTieringConfiguration.html)       | :x:                                       |
| [GetBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketInventoryConfiguration.html)                         | :x:                                       |
| [GetBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle.html)                                                   | :x: - Deprecated in S3 API                |
| [GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)                         | :white_check_mark:                        |
| [GetBucketLocation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html)                                                     | :x:                                       |
| [GetBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html)                                                       | :x:                                       |
| [GetBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketMetricsConfiguration.html)                             | :x:                                       |
| [GetBucketNotification](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotification.html)                                             | :x:                                       |
| [GetBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html)                   | :x:                                       |
| [GetBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html)                                   | :x:                                       |
| [GetBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html)                                                         | :x:                                       |
| [GetBucketPolicyStatus](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html)                                             | :x:                                       |
| [GetBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html)                                               | :x:                                       |
| [GetBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html)                                         | :x:                                       |
| [GetBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html)                                                       | :x:                                       |
| [GetBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html)                                                 | :x:                                       |
| [GetBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketWebsite.html)                                                       | :x:                                       |
| [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)                                                                     | :white_check_mark:                        |
| [GetObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html)                                                               | :white_check_mark:                        |
| [GetObjectAttributes](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html)                                                 | :x:                                       |
| [GetObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html)                                                   | :white_check_mark:                        |
| [GetObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html)                                   | :white_check_mark:                        |
| [GetObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html)                                                   | :white_check_mark:                        |
| [GetObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html)                                                       | :white_check_mark:                        |
| [GetObjectTorrent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html)                                                       | :x:                                       |
| [GetPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html)                                               | :x:                                       |
| [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)                                                                   | :white_check_mark:                        |
| [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)                                                                   | :white_check_mark:                        |
| [ListBucketAnalyticsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketAnalyticsConfigurations.html)                     | :x:                                       |
| [ListBucketIntelligentTieringConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketIntelligentTieringConfigurations.html)   | :x:                                       |
| [ListBucketInventoryConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketInventoryConfigurations.html)                     | :x:                                       |
| [ListBucketMetricsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketMetricsConfigurations.html)                         | :x:                                       |
| [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)                                                                 | :white_check_mark:                        |
| [ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)                                               | :white_check_mark:                        |
| [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)                                                                 | :white_check_mark: - Deprecated in S3 API |
| [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)                                                             | :white_check_mark:                        |
| [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)                                                   | :x:                                       |
| [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)                                                                     | :white_check_mark:                        |
| [PutBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAccelerateConfiguration.html)                       | :x:                                       |
| [PutBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html)                                                               | :x:                                       |
| [PutBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAnalyticsConfiguration.html)                         | :x:                                       |
| [PutBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html)                                                             | :x:                                       |
| [PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html)                                                 | :x:                                       |
| [PutBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketIntelligentTieringConfiguration.html)       | :x:                                       |
| [PutBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketInventoryConfiguration.html)                         | :x:                                       |
| [PutBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html)                                                   | :x: - Deprecated in S3 API                |
| [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)                         | :white_check_mark:                        |
| [PutBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html)                                                       | :x:                                       |
| [PutBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketMetricsConfiguration.html)                             | :x:                                       |
| [PutBucketNotification](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotification.html)                                             | :x:                                       |
| [PutBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html)                   | :x:                                       |
| [PutBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html)                                   | :x:                                       |
| [PutBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html)                                                         | :x:                                       |
| [PutBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html)                                               | :x:                                       |
| [PutBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html)                                         | :x:                                       |
| [PutBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html)                                                       | :x:                                       |
| [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)                                                 | :x:                                       |
| [PutBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html)                                                       | :x:                                       |
| [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)                                                                     | :white_check_mark:                        |
| [PutObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html)                                                               | :white_check_mark:                        |
| [PutObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)                                                   | :white_check_mark:                        |
| [PutObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html)                                   | :white_check_mark:                        |
| [PutObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)                                                   | :white_check_mark:                        |
| [PutObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html)                                                       | :white_check_mark:                        |
| [PutPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutPublicAccessBlock.html)                                               | :x:                                       |
| [RestoreObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html)                                                             | :x:                                       |
| [SelectObjectContent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html)                                                 | :x:                                       |
| [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)                                                                   | :white_check_mark:                        |
| [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)                                                           | :white_check_mark:                        |
| [WriteGetObjectResponse](https://docs.aws.amazon.com/AmazonS3/latest/API/API_WriteGetObjectResponse.html)                                           | :x:                                       |

## File System Structure
S3Mock stores Buckets, Objects, Parts and other data on disk.  
This lets users inspect the stored data while the S3Mock is running.  
If the config property `retainFilesOnExit` is set to `true`, this data will not be deleted when S3Mock is shut down.

| :exclamation: FYI                                                                                                                                                                                                                                   |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| While it _may_ be possible to start S3Mock on a root folder from a previous run and have all data available through the S3 API, the structure and contents of the files are not considered Public API, and are subject to change in later releases. |
| Also, there are no automated test cases for this behaviour.                                                                                                                                                                                         |

### Root-Folder
S3Mock stores buckets and objects a root-folder.

This folder is expected to be empty when S3Mock starts. See also FYI above.
```
/<root-folder>/
```

### Buckets
Buckets are stored as a folder with their name as created through the S3 API directly below the root:
```
/<root-folder>/<bucket-name>/
```
[BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) is stored in a file in the bucket directory, serialized as JSON.  
[BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) contains the "key" -> "uuid" dictionary for all objects uploaded to this bucket among other data.
```
/<root-folder>/<bucket-name>/bucketMetadata
```
### Objects

Objects are stored in folders below the bucket they were created in.
A folder is created that uses the Object's UUID assigned in the [BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) as a name.
```
/<root-folder>/<bucket-name>/<uuid>/
```
Object data is stored below that UUID folder.

Binary data is always stored in a file `binaryData`
```
/<root-folder>/<bucket-name>/<uuid>/binaryData
```

[Object metadata](server/src/main/java/com/adobe/testing/s3mock/store/S3ObjectMetadata.java) is serializes as JSON and stored as `objectMetadata`
```
/<root-folder>/<bucket-name>/<uuid>/objectMetadata
```

### Multipart Uploads

Multipart Uploads are created in a bucket using object keys and an uploadId.  
The object is assigned a UUID within the bucket (stored in [BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java)).  
The [Multipart upload metadata](server/src/main/java/com/adobe/testing/s3mock/store/MultipartUploadInfo.java) is currently not stored on disk.

The parts folder is created below the object UUID folder named with the `uploadId`:
```
/<root-folder>/<bucket-name>/<uuid>/<uploadId>/
```

Each part is stored in the parts folder with the `partNo` as name and `.part` as a suffix.
```
/<root-folder>/<bucket-name>/<uuid>/<uploadId>/<partNo>.part
```

## Usage

### Configuration

The mock can be configured with the following environment parameters:

- `validKmsKeys`: list of KMS Key-Refs that are to be treated as *valid*.
  - KMS keys must be configured as valid ARNs in the format of "`arn:aws:kms:region:acct-id:key/key-id`", for example "`arn:aws:kms:us-east-1:1234567890:key/valid-test-key-id`"
  - The list must be comma separated keys like `arn-1, arn-2`
  - When requesting with KMS encryption, the key ID is passed to the SDK / CLI, in the example above this would be "`valid-test-key-id`".
  - *S3Mock does not implement KMS encryption*, if a key ID is passed in a request, S3Mock will just validate if a given Key was configured during startup and reject the request if the given Key was not configured.
- `initialBuckets`: list of names for buckets that will be available initially.
  - The list must be comma separated names like `bucketa, bucketb`
- `root`: the base directory to place the temporary files exposed by the mock.
- `debug`: set to `true` to enable [Spring Boot's debug output](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.logging.console-output).
- `trace`: set to `true` to enable  [Spring Boot's trace output](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.logging.console-output).
- `retainFilesOnExit`: set to `true` to let S3Mock keep all files that were created during its lifetime. Default is `false`, all files are removed if S3Mock shuts down.

### S3Mock Docker

The `S3Mock` Docker container is the recommended way to use `S3Mock`.  
It is released to [Docker Hub](https://hub.docker.com/r/adobe/s3mock).  
The container is lightweight, built on top of the official [Linux Alpine image](https://hub.docker.com/_/alpine).

If needed, configure [memory](https://docs.docker.com/engine/reference/commandline/run/#specify-hard-limits-on-memory-available-to-containers--m---memory) and [cpu](https://docs.docker.com/engine/reference/commandline/run/#options) limits for the S3Mock docker container.

The JVM will automatically use half the available memory.

#### Start using the command-line

Starting on the command-line:

    docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock

The port `9090` is for HTTP, port `9191` is for HTTPS.

#### Start using the Fabric8 Docker-Maven-Plugin

Our [integration tests](integration-tests) are using the Amazon S3 Client to verify the server functionality against the S3Mock. During the Maven build, the Docker image is started using the [docker-maven-plugin](https://dmp.fabric8.io/) and the corresponding ports are passed to the JUnit test through the `maven-failsafe-plugin`. See [`AmazonClientUploadIT`](integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/AmazonClientUploadV1IT.kt) how it's used in the code.

This way, one can easily switch between calling the S3Mock or the real S3 endpoint and this doesn't add any additional Java dependencies to the project.

#### Start using Testcontainers

The [`S3MockContainer`](testsupport/testcontainers/src/main/java/com/adobe/testing/s3mock/testcontainers/S3MockContainer.java) is a `Testcontainer` implementation that comes pre-configured exposing HTTP and HTTPS ports. Environment variables can be set on startup.

The example [`S3MockContainerJupiterTest`](testsupport/testcontainers/src/test/java/com/adobe/testing/s3mock/testcontainers/S3MockContainerJupiterTest.java) demonstrates the usage with JUnit 5.  The example [`S3MockContainerManualTest`](testsupport/testcontainers/src/test/java/com/adobe/testing/s3mock/testcontainers/S3MockContainerManualTest.java) demonstrates the usage with plain Java.

Testcontainers provides integrations for JUnit 4, JUnit 5 and Spock.  
For more information, visit the [Testcontainers](https://www.testcontainers.org/) website.

To use the [`S3MockContainer`](testsupport/testcontainers/src/main/java/com/adobe/testing/s3mock/testcontainers/S3MockContainer.java), use the following Maven artifact in `test` scope:

```xml
<dependency>
 <groupId>com.adobe.testing</groupId>
 <artifactId>s3mock-testcontainers</artifactId>
 <version>...</version>
 <scope>test</scope>
</dependency>
```

### S3Mock Java

`S3Mock` Java libraries are released and published to the Sonatype Maven Repository and subsequently published to
the official [Maven mirrors](https://search.maven.org/search?q=g:com.adobe.testing%20a:s3mock).

| :warning: WARNING                                                                                 |
|:--------------------------------------------------------------------------------------------------|
| Using the Java libraries is **discouraged**, see explanation below                                |
| Using the Docker image is **encouraged** to insulate both S3Mock and your application at runtime. |

`S3Mock` is built using Spring Boot, if projects use `S3Mock` by adding the dependency to their project and starting
the `S3Mock` during a JUnit test, classpaths of the tested application and of the `S3Mock` are merged, leading
to unpredictable and undesired effects such as class conflicts or dependency version conflicts.  
This is especially problematic if the tested application itself is a Spring (Boot) application, as both applications will load configurations based on availability of certain classes in the classpath, leading to unpredictable runtime behaviour.

_This is the opposite of what software engineers are trying to achieve when thoroughly testing code in continuous integration..._

`S3Mock` dependencies are updated regularly, any update could break any number of projects.  
**See also [issues labelled "dependency-problem"](https://github.com/adobe/S3Mock/issues?q=is%3Aissue+label%3Adependency-problem).**

**See also [the Java section below](#Java)**

#### Start using the JUnit4 Rule

The example [`S3MockRuleTest`](testsupport/junit4/src/test/java/com/adobe/testing/s3mock/junit4/S3MockRuleTest.java) demonstrates the usage of the `S3MockRule`, which can be configured through a _builder_.

To use the JUnit4 Rule, use the following Maven artifact in `test` scope:

```xml
<dependency>
 <groupId>com.adobe.testing</groupId>
 <artifactId>s3mock-junit4</artifactId>
 <version>...</version>
 <scope>test</scope>
</dependency>
```

#### Start using the JUnit5 Extension

The `S3MockExtension` can currently be used in two ways:

1. Declaratively using `@ExtendWith(S3MockExtension.class)` and by injecting a properly configured instance of `AmazonS3` client and/or the started `S3MockApplication` to the tests.
See examples: [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk1/S3MockExtensionDeclarativeTest.java)  (for SDKv1)
or [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk2/S3MockExtensionDeclarativeTest.java) (for SDKv2)

2. Programmatically using `@RegisterExtension` and by creating and configuring the `S3MockExtension` using a _builder_.
See examples: [`S3MockExtensionProgrammaticTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk1/S3MockExtensionProgrammaticTest.java) (for SDKv1)
or [`S3MockExtensionProgrammaticTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk2/S3MockExtensionProgrammaticTest.java) (for SDKv2)

To use the JUnit5 Extension, use the following Maven artifact in `test` scope:

```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-junit5</artifactId>
  <version>...</version>
  <scope>test</scope>
</dependency>
```

#### Start using the TestNG Listener

The example [`S3MockListenerXMLConfigurationTest`](testsupport/testng/src/test/java/com/adobe/testing/s3mock/testng/S3MockListenerXmlConfigurationTest.java) demonstrates the usage of the `S3MockListener`, which can be configured as shown in [`testng.xml`](testsupport/testng/src/test/resources/testng.xml). The listener bootstraps S3Mock application before TestNG execution starts and shuts down the application just before the execution terminates. Please refer to [`IExecutionListener`](https://jitpack.io/com/github/cbeust/testng/main/javadoc/org/testng/IExecutionListener.html)

To use the TestNG Listener, use the following Maven artifact in `test` scope:

```xml
<dependency>
 <groupId>com.adobe.testing</groupId>
 <artifactId>s3mock-testng</artifactId>
 <version>...</version>
 <scope>test</scope>
</dependency>
```

#### Start programmatically

Include the following dependency and use one of the `start` methods in `com.adobe.testing.s3mock.S3MockApplication`:

```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock</artifactId>
  <version>...</version>
</dependency>
```

## Build & Run

To build this project, you need Docker, JDK 8 or higher, and Maven:

    ./mvnw clean install

If you want to skip the Docker build, pass the optional parameter "skipDocker":

    ./mvnw clean install -DskipDocker

You can run the S3Mock from the sources by either of the following methods:

* Run or Debug the class `com.adobe.testing.s3mock.S3MockApplication` in the IDE.
* using Docker:
  * `./mvnw clean package -pl server -am -DskipTests`
  * `docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock:latest`
* using the Docker Maven plugin:
  * `./mvnw clean package docker:start -pl server -am -DskipTests -Ddocker.follow -Dit.s3mock.port_http=9090 -Dit.s3mock.port_https=9191` (stop with `ctrl-c`)

Once the application is started, you can execute the `*IT` tests from your IDE.

### Java
This repo is built with Java 17, output is _currently_ bytecode compatible with Java 8.

This will change with Spring Boot 3 and Spring Framework 6, [these releases raise the baseline Java version to 17](https://spring.io/blog/2022/05/24/preparing-for-spring-boot-3-0).  
Once `S3Mock` updates (probably in early 2023), our Java baseline will raise to 17 as well.  
This will make `S3Mock` incompatible for all customer applications using the Java integration, but an older Java version like Java 8 or Java 11.

Most likely, version 2.x of the S3Mock will be branched off and maintained for critical fixes, while
version 3.x will be released from the `main` branch and further refactored and updated, but released and/or supported only as a Docker container.

### Kotlin
The [Integration Tests](integration-tests) are built in Kotlin.

## Contributing

Contributions are welcomed! Read the [Contributing Guide](./.github/CONTRIBUTING.md) for more information.

## Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.
