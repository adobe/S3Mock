[![Latest Version](https://img.shields.io/maven-central/v/com.adobe.testing/s3mock.svg?maxAge=3600&label=Latest%20Release)](https://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.adobe.testing%20a%3As3mock)
![Maven Build](https://github.com/adobe/S3Mock/workflows/Maven%20Build/badge.svg)
[![Docker Hub](https://img.shields.io/badge/docker-latest-blue.svg)](https://hub.docker.com/r/adobe/s3mock/)
[![Docker Pulls](https://img.shields.io/docker/pulls/adobe/s3mock)](https://hub.docker.com/r/adobe/s3mock)
[![Kotlin](https://img.shields.io/badge/MADE%20with-Kotlin-RED.svg)](#Kotlin)  
[![Java](https://img.shields.io/badge/MADE%20with-Java-RED.svg)](#Java)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7673/badge)](https://bestpractices.coreinfrastructure.org/projects/7673)
[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/adobe/S3Mock?label=openssf%20scorecard&style=flat)](https://api.securityscorecards.dev/projects/github.com/adobe/S3Mock)
[![GitHub stars](https://img.shields.io/github/stars/adobe/S3Mock.svg?style=social&label=Star&maxAge=2592000)](https://github.com/adobe/S3Mock/stargazers/)

<!-- TOC -->
  * [S3Mock](#s3mock)
  * [Changelog](#changelog)
  * [Supported S3 operations](#supported-s3-operations)
  * [Usage](#usage)
    * [Usage of AWS S3 SDKs](#usage-of-aws-s3-sdks)
      * [Path-style vs. Domain-style access](#path-style-vs-domain-style-access)
      * [Presigned URLs](#presigned-urls)
      * [Self-signed SSL certificate](#self-signed-ssl-certificate)
    * [Usage of AWS CLI](#usage-of-aws-cli)
      * [Create bucket](#create-bucket)
      * [Put object](#put-object)
      * [Get object](#get-object)
      * [Get object using HTTPS](#get-object-using-https)
    * [Usage of plain HTTP / HTTPS with cURL](#usage-of-plain-http--https-with-curl)
      * [Create bucket](#create-bucket-1)
      * [Put object](#put-object-1)
      * [Get object](#get-object-1)
      * [Get object using HTTPS](#get-object-using-https-1)
    * [S3Mock configuration options](#s3mock-configuration-options)
    * [S3Mock Docker](#s3mock-docker)
      * [Start using the command-line](#start-using-the-command-line)
      * [Start using the Fabric8 Docker-Maven-Plugin](#start-using-the-fabric8-docker-maven-plugin)
      * [Start using Testcontainers](#start-using-testcontainers)
      * [Start using Docker compose](#start-using-docker-compose)
        * [Simple example](#simple-example)
        * [Expanded example](#expanded-example)
      * [Start using a self-signed SSL certificate](#start-using-a-self-signed-ssl-certificate)
    * [S3Mock Java](#s3mock-java)
      * [Start using the JUnit4 Rule](#start-using-the-junit4-rule)
      * [Start using the JUnit5 Extension](#start-using-the-junit5-extension)
      * [Start using the TestNG Listener](#start-using-the-testng-listener)
      * [Start programmatically](#start-programmatically)
  * [File System Structure](#file-system-structure)
    * [Root-Folder](#root-folder)
    * [Buckets](#buckets)
    * [Objects](#objects)
      * [Object versions](#object-versions)
    * [Multipart Uploads](#multipart-uploads)
  * [Build & Run](#build--run)
    * [Build](#build)
      * [With Docker](#with-docker)
      * [Without Docker](#without-docker)
    * [Run](#run)
      * [Spring Application](#spring-application)
      * [Docker](#docker)
      * [Docker Maven plugin](#docker-maven-plugin)
    * [Run integration tests](#run-integration-tests)
    * [Java](#java)
    * [Kotlin](#kotlin)
  * [Governance model](#governance-model)
  * [Vulnerability reports](#vulnerability-reports)
  * [Security](#security)
  * [Contributing](#contributing)
  * [Licensing](#licensing)
  * [Powered by](#powered-by)
  * [Star History](#star-history)
<!-- TOC -->

## S3Mock

`S3Mock` is a lightweight server that implements parts of
the [Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html).  
It has been created to support local integration testing by reducing infrastructure dependencies.

The `S3Mock` server can be started as a standalone *Docker* container, using *Testcontainers*, *JUnit4*, *JUnit5* and
*TestNG* support, or programmatically.

## Changelog

See [GitHub releases](https://github.com/adobe/S3Mock/releases).  
See also the [changelog](CHANGELOG.md) for detailed information about changes in releases and changes planned for future
releases.

## Supported S3 operations

Of
these [operations of the Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html),
all marked :white_check_mark: are supported by S3Mock:

| Operation                                                                                                                                           | Support            | Comment                |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|------------------------|
| [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)                                               | :white_check_mark: |                        |
| [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)                                         | :white_check_mark: |                        |
| [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)                                                                   | :white_check_mark: |                        |
| [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)                                                               | :white_check_mark: |                        |
| [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)                                             | :white_check_mark: |                        |
| [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)                                                               | :white_check_mark: |                        |
| [DeleteBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketAnalyticsConfiguration.html)                   | :x:                |                        |
| [DeleteBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html)                                                       | :x:                |                        |
| [DeleteBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html)                                           | :x:                |                        |
| [DeleteBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketIntelligentTieringConfiguration.html) | :x:                |                        |
| [DeleteBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketInventoryConfiguration.html)                   | :x:                |                        |
| [DeleteBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html)                                             | :white_check_mark: |                        |
| [DeleteBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketMetricsConfiguration.html)                       | :x:                |                        |
| [DeleteBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html)                             | :x:                |                        |
| [DeleteBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html)                                                   | :x:                |                        |
| [DeleteBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketReplication.html)                                         | :x:                |                        |
| [DeleteBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html)                                                 | :x:                |                        |
| [DeleteBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketWebsite.html)                                                 | :x:                |                        |
| [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)                                                               | :white_check_mark: |                        |
| [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)                                                             | :white_check_mark: |                        |
| [DeleteObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html)                                                 | :white_check_mark: |                        |
| [DeletePublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletePublicAccessBlock.html)                                         | :x:                |                        |
| [GetBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAccelerateConfiguration.html)                       | :x:                |                        |
| [GetBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html)                                                               | :x:                |                        |
| [GetBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAnalyticsConfiguration.html)                         | :x:                |                        |
| [GetBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html)                                                             | :x:                |                        |
| [GetBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html)                                                 | :x:                |                        |
| [GetBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketIntelligentTieringConfiguration.html)       | :x:                |                        |
| [GetBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketInventoryConfiguration.html)                         | :x:                |                        |
| [GetBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle.html)                                                   | :x:                | Deprecated in S3 API   |
| [GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)                         | :white_check_mark: |                        |
| [GetBucketLocation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html)                                                     | :white_check_mark: |                        |
| [GetBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html)                                                       | :x:                |                        |
| [GetBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketMetricsConfiguration.html)                             | :x:                |                        |
| [GetBucketNotification](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotification.html)                                             | :x:                |                        |
| [GetBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html)                   | :x:                |                        |
| [GetBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html)                                   | :x:                |                        |
| [GetBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html)                                                         | :x:                |                        |
| [GetBucketPolicyStatus](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html)                                             | :x:                |                        |
| [GetBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html)                                               | :x:                |                        |
| [GetBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html)                                         | :x:                |                        |
| [GetBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html)                                                       | :x:                |                        |
| [GetBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html)                                                 | :white_check_mark: |                        |
| [GetBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketWebsite.html)                                                       | :x:                |                        |
| [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)                                                                     | :white_check_mark: |                        |
| [GetObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html)                                                               | :white_check_mark: |                        |
| [GetObjectAttributes](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html)                                                 | :white_check_mark: | for objects, not parts |
| [GetObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html)                                                   | :white_check_mark: |                        |
| [GetObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html)                                   | :white_check_mark: |                        |
| [GetObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html)                                                   | :white_check_mark: |                        |
| [GetObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html)                                                       | :white_check_mark: |                        |
| [GetObjectTorrent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html)                                                       | :x:                |                        |
| [GetPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html)                                               | :x:                |                        |
| [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)                                                                   | :white_check_mark: |                        |
| [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)                                                                   | :white_check_mark: |                        |
| [ListBucketAnalyticsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketAnalyticsConfigurations.html)                     | :x:                |                        |
| [ListBucketIntelligentTieringConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketIntelligentTieringConfigurations.html)   | :x:                |                        |
| [ListBucketInventoryConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketInventoryConfigurations.html)                     | :x:                |                        |
| [ListBucketMetricsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketMetricsConfigurations.html)                         | :x:                |                        |
| [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)                                                                 | :white_check_mark: |                        |
| [ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)                                               | :white_check_mark: |                        |
| [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)                                                                 | :white_check_mark: | Deprecated in S3 API   |
| [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)                                                             | :white_check_mark: |                        |
| [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)                                                   | :white_check_mark: |                        |
| [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)                                                                     | :white_check_mark: |                        |
| [PostObject](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html)                                                                   | :white_check_mark: |                        |
| [PutBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAccelerateConfiguration.html)                       | :x:                |                        |
| [PutBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html)                                                               | :x:                |                        |
| [PutBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAnalyticsConfiguration.html)                         | :x:                |                        |
| [PutBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html)                                                             | :x:                |                        |
| [PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html)                                                 | :x:                |                        |
| [PutBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketIntelligentTieringConfiguration.html)       | :x:                |                        |
| [PutBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketInventoryConfiguration.html)                         | :x:                |                        |
| [PutBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html)                                                   | :x:                | Deprecated in S3 API   |
| [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)                         | :white_check_mark: |                        |
| [PutBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html)                                                       | :x:                |                        |
| [PutBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketMetricsConfiguration.html)                             | :x:                |                        |
| [PutBucketNotification](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotification.html)                                             | :x:                |                        |
| [PutBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html)                   | :x:                |                        |
| [PutBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html)                                   | :x:                |                        |
| [PutBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html)                                                         | :x:                |                        |
| [PutBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html)                                               | :x:                |                        |
| [PutBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html)                                         | :x:                |                        |
| [PutBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html)                                                       | :x:                |                        |
| [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)                                                 | :white_check_mark: |                        |
| [PutBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html)                                                       | :x:                |                        |
| [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)                                                                     | :white_check_mark: |                        |
| [PutObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html)                                                               | :white_check_mark: |                        |
| [PutObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)                                                   | :white_check_mark: |                        |
| [PutObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html)                                   | :white_check_mark: |                        |
| [PutObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html)                                                   | :white_check_mark: |                        |
| [PutObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html)                                                       | :white_check_mark: |                        |
| [PutPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutPublicAccessBlock.html)                                               | :x:                |                        |
| [RestoreObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html)                                                             | :x:                |                        |
| [SelectObjectContent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html)                                                 | :x:                |                        |
| [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)                                                                   | :white_check_mark: |                        |
| [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)                                                           | :white_check_mark: |                        |
| [WriteGetObjectResponse](https://docs.aws.amazon.com/AmazonS3/latest/API/API_WriteGetObjectResponse.html)                                           | :x:                |                        |

## Usage

### Usage of AWS S3 SDKs

S3Mock can be used with any of the available AWS S3 SDKs.

The [Integration Tests](integration-tests) and tests in [testsupport](testsupport) contain various examples of how to use the S3Mock with the AWS SDK for Java
v1 and v2 in Kotlin.

`S3Client` or `S3Presigner` instances are created here:

* Kotlin: [Integration Test base class](integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/S3TestBase.kt)
* Java: [S3Mock unit test starter base class](testsupport/common/src/main/java/com/adobe/testing/s3mock/testsupport/common/S3MockStarter.java)

#### Path-style vs. Domain-style access

AWS S3 SDKs usually use domain-style access by default. Configuration is needed for path-style access.

S3Mock currently only supports path-style access (e.g., `http://localhost:9090/bucket/someKey`).

Domain-style access to buckets (e.g., `http://bucket.localhost:9090/someKey`) does not work because the domain
`localhost` is special and does not allow for subdomain access without
modifications to the operating system.

#### Presigned URLs

S3 SDKs can be used
to [create presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html), the S3 API
supports access through those URLs.

S3Mock will accept presigned URLs, but it *ignores all parameters*.  
For instance, S3Mock does not verify the HTTP verb that the presigned uri was created with, and it does not validate
whether the link is expired or not.

S3 SDKs can be used to create presigned URLs pointing to S3Mock if they're configured for path-style access. See the
"Usage..." section above for links to examples on how to use the SDK with presigned URLs.

#### Self-signed SSL certificate

S3Mock supports connections via HTTP and HTTPS. It includes a self-signed SSL certificate which is rejected by most HTTP
clients by default.
To use HTTPS, the client must accept the self-signed certificate.

On command line, this can be done by setting the `--no-verify-ssl` option in the AWS CLI or by using the `--insecure`
option in cURL, see below.

Java and Kotlin SDKs can be configured to trust any SSL certificate, see links to `S3Client` creation above.

### Usage of AWS CLI

S3Mock can be used with the AWS CLI. Setting the `--endpoint-url` enables path-style access, `--no-verify-ssl` is needed
for HTTPS access.

Examples:

#### Create bucket

```shell
aws s3api create-bucket --bucket my-bucket --endpoint-url=http://localhost:9090
```

#### Put object

```shell
aws s3api put-object --bucket my-bucket --key my-file --body ./my-file --endpoint-url=http://localhost:9090
```

#### Get object

```shell
aws s3api get-object --bucket my-bucket --key my-file --endpoint-url=http://localhost:9090 my-file-output
```

#### Get object using HTTPS

```shell
aws s3api get-object --bucket my-bucket --key my-file --no-verify-ssl --endpoint-url=https://localhost:9191 my-file-output
```

### Usage of plain HTTP / HTTPS with cURL

As long as the requests work with the S3 API, they will work with S3Mock as well. Use `--insecure` to ignore SSL errors.

Examples:

#### Create bucket

```shell
curl --request PUT "http://localhost:9090/my-test-bucket/"
```

#### Put object

```shell
curl --request PUT --upload-file ./my-file http://localhost:9090/my-test-bucket/my-file
```

#### Get object

```shell
curl --request GET http://localhost:9090/my-test-bucket/my-file -O
```

#### Get object using HTTPS

```shell
curl --insecure --request GET https://localhost:9191/my-test-bucket/my-file -O
```

### S3Mock configuration options

The mock can be configured with the following environment variables:

- `COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS`: list of KMS Key-Refs that are to be treated as *valid*.
  - Legacy name: `validKmsKeys`
  - Default: none
  - KMS keys must be configured as valid ARNs in the format of "`arn:aws:kms:region:acct-id:key/key-id`", for example "
    `arn:aws:kms:us-east-1:1234567890:key/valid-test-key-id`"
  - The list must be comma separated keys like `arn-1, arn-2`
  - When requesting with KMS encryption, the key ID is passed to the SDK / CLI, in the example above this would be "
    `valid-test-key-id`".
  - *S3Mock does not implement KMS encryption*, if a key ID is passed in a request, S3Mock will just validate if a given
    Key was configured during startup and reject the request if the given Key was not configured.
- `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS`: list of names for buckets that will be available initially.
  - Legacy name: `initialBuckets`
  - Default: none
  - The list must be comma separated names like `bucketa, bucketb`
- `COM_ADOBE_TESTING_S3MOCK_STORE_REGION`: the region the S3Mock is supposed to mock.
  - Legacy name: `COM_ADOBE_TESTING_S3MOCK_REGION`
  - Example: `eu-west-1`
  - Default: `us-east-1`
- `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT`: the base directory to place the temporary files exposed by the mock. If S3Mock is started in Docker, a volume
  must be mounted as the `root` directory, see examples below.
  - Legacy name: `root`
  - Default: Java temp directory
- `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT`: set to `true` to let S3Mock keep all files that were created during its lifetime. Default is
  `false`, all files are removed if S3Mock shuts down.
  - Legacy name: `retainFilesOnExit`
  - Default: false
- `debug`: set to `true` to
  enable [Spring Boot's debug output](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.logging.console-output).
- `trace`: set to `true` to
  enable [Spring Boot's trace output](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.logging.console-output).

### S3Mock Docker

The `S3Mock` Docker container is the recommended way to use `S3Mock`.  
It is released to [Docker Hub](https://hub.docker.com/r/adobe/s3mock).  
The container is lightweight, built on top of the official [Linux Alpine image](https://hub.docker.com/_/alpine).

If needed,
configure [memory](https://docs.docker.com/engine/reference/commandline/run/#specify-hard-limits-on-memory-available-to-containers--m---memory)
and [cpu](https://docs.docker.com/engine/reference/commandline/run/#options) limits for the S3Mock Docker container.

The JVM will automatically use half the available memory.

#### Start using the command-line

Starting on the command-line:

```shell
docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock
```

The port `9090` is for HTTP, port `9191` is for HTTPS.

Example with configuration via environment variables:

```shell
docker run -p 9090:9090 -p 9191:9191 -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=test -e debug=true -t adobe/s3mock
```

#### Start using the Fabric8 Docker-Maven-Plugin

Our [integration tests](integration-tests) are using the Amazon S3 Client to verify the server functionality against the
S3Mock. During the Maven build, the Docker image is started using the [docker-maven-plugin](https://dmp.fabric8.io/) and
the corresponding ports are passed to the JUnit test through the `maven-failsafe-plugin`. See [`BucketIT`](integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/BucketIT.kt)
as an example on how it's used in the code.

This way, one can easily switch between calling the S3Mock or the real S3 endpoint, and this doesn't add any additional
Java dependencies to the project.

#### Start using Testcontainers

The [`S3MockContainer`](testsupport/testcontainers/src/main/java/com/adobe/testing/s3mock/testcontainers/S3MockContainer.java)
is a `Testcontainer` implementation that comes pre-configured exposing HTTP and HTTPS ports. Environment variables can
be set on startup.

The example [`S3MockContainerJupiterTest`](testsupport/testcontainers/src/test/kotlin/com/adobe/testing/s3mock/testcontainers/S3MockContainerJupiterTest.kt)
demonstrates the usage with JUnit 5. The example [`S3MockContainerManualTest`](testsupport/testcontainers/src/test/kotlin/com/adobe/testing/s3mock/testcontainers/S3MockContainerManualTest.kt)
demonstrates the usage with plain Kotlin. Java will be similar.

Testcontainers provide integrations for JUnit 4, JUnit 5 and Spock.  
For more information, visit the [Testcontainers](https://www.testcontainers.org/) website.

To use the [
`S3MockContainer`](testsupport/testcontainers/src/main/java/com/adobe/testing/s3mock/testcontainers/S3MockContainer.java),
use the following Maven artifact in `test` scope:

```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-testcontainers</artifactId>
  <version>...</version>
  <scope>test</scope>
</dependency>
```

#### Start using Docker compose

##### Simple example

Create a file `docker-compose.yml`

```yaml
services:
  s3mock:
    image: adobe/s3mock:latest
    environment:
      - COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=bucket1
    ports:
      - 9090:9090
```

Start with

```shell
docker compose up -d
```

Stop with

```shell
docker compose down
```

##### Expanded example

Suppose we want to see what S3Mock is persisting and look at the logs it generates in detail.

A local directory is needed, let's call it `locals3root`. This directory must be mounted as a volume into the Docker
container when it's started, and that mounted volume must then be configured as the `root` for S3Mock. Let's call the
mounted volume inside the container `containers3root`. S3Mock will delete all files when it shuts down,
`COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT=true` tells it to leave all files instead.

Also, to see debug logs, `debug=true` must be configured for S3Mock.

Create a file `docker-compose.yml`

```yaml
services:
  s3mock:
    image: adobe/s3mock:latest
    environment:
      - debug=true
      - COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT=true
      - COM_ADOBE_TESTING_S3MOCK_STORE_ROOT=containers3root
    ports:
      - 9090:9090
      - 9191:9191
    volumes:
      - ./locals3root:/containers3root
```

Create a directory `locals3root`.

Start with

```shell
docker compose up -d
```

Create a bucket "my-test-bucket" with

```shell
curl --request PUT "http://localhost:9090/my-test-bucket/"
```

Stop with

```shell
docker compose down
```

Look into the directory `locals3root` where metadata and contents of the bucket are stored.

```shell
$ mkdir s3mock-mounttest
$ cd s3mock-mounttest
$ mkdir locals3root
$ cat docker-compose.yml
services:
  s3mock:
    image: adobe/s3mock:latest
    environment:
      - debug=true
      - COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT=true
      - COM_ADOBE_TESTING_S3MOCK_STORE_ROOT=containers3root
    ports:
      - 9090:9090
      - 9191:9191
    volumes:
      - ./locals3root:/containers3root

$ docker compose up -d
[+] Running 2/2
 ✔ Network s3mock-mounttest_default     Created
 ✔ Container s3mock-mounttest-s3mock-1  Started
$ curl --request PUT "http://localhost:9090/my-test-bucket/"
$ docker compose down
[+] Running 2/0
 ✔ Container s3mock-mounttest-s3mock-1  Removed
 ✔ Network s3mock-mounttest_default     Removed
 
$ ls locals3root
my-test-bucket
$ ls locals3root/my-test-bucket
bucketMetadata.json
```

#### Start using a self-signed SSL certificate

S3Mock includes a self-signed SSL certificate:

```shell
$ curl -vvv --insecure --request GET https://localhost:9191/my-test-bucket/my-file -O
[...]
* Server certificate:
*  subject: C=DE; ST=Hamburg; L=Hamburg; O=S3Mock; OU=S3Mock; CN=Adobe S3Mock
*  start date: Jul 25 12:28:53 2022 GMT
*  expire date: Nov 25 12:28:53 3021 GMT
*  issuer: C=DE; ST=Hamburg; L=Hamburg; O=S3Mock; OU=S3Mock; CN=Adobe S3Mock
*  SSL certificate verify result: self signed certificate (18), continuing anyway.
[...]
```

To use a custom self-signed SSL certificate, derive your own Docker container from the S3Mock container:

```dockerfile
FROM adobe/s3mock:4.2.0

ENV server.ssl.key-store=/opt/customcert.jks
ENV server.ssl.key-store-password=password
ENV server.ssl.key-alias=selfsigned

RUN keytool -genkey -keyalg RSA -alias selfsigned \
  -validity 360 \
  -keystore /opt/customcert.jks \
  -dname "cn=Test, ou=Test, o=Docker, l=NY, st=NY, c=US" \
  -storepass password -keysize 2048 \
  -ext "san=dns:localhost"
```

```shell
$ curl -vvv --insecure --request GET https://localhost:9191/my-test-bucket/my-file -O
[...]
* Server certificate:
*  subject: C=US; ST=NY; L=NY; O=Docker; OU=Test; CN=Test
*  start date: May  9 14:33:40 2025 GMT
*  expire date: May  4 14:33:40 2026 GMT
*  issuer: C=US; ST=NY; L=NY; O=Docker; OU=Test; CN=Test
*  SSL certificate verify result: self signed certificate (18), continuing anyway.
[...]
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
This is especially problematic if the tested application itself is a Spring (Boot) application, as both applications
will load configurations based on the availability of certain classes in the classpath, leading to unpredictable runtime
behavior.

_This is the opposite of what software engineers are trying to achieve when thoroughly testing code in continuous
integration..._

`S3Mock` dependencies are updated regularly, any update could break any number of projects.  
**See also [issues labeled "dependency-problem"](https://github.com/adobe/S3Mock/issues?q=is%3Aissue+label%3Adependency-problem).**

**See also [the Java section below](#Java)**

#### Start using the JUnit4 Rule

The example [`S3MockRuleTest`](testsupport/junit4/src/test/java/com/adobe/testing/s3mock/junit4/S3MockRuleTest.java)
demonstrates the usage of the `S3MockRule`, which can be configured through a _builder_.

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

1. Declaratively using `@ExtendWith(S3MockExtension.class)` and by injecting a properly configured instance of
   `AmazonS3` client and/or the started `S3MockApplication` to the tests.
   See examples: [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk1/S3MockExtensionDeclarativeTest.java)  (for SDKv1)
   or [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/kotlin/com/adobe/testing/s3mock/junit5/sdk2/S3MockExtensionDeclarativeTest.kt) (for SDKv2)

2. Programmatically using `@RegisterExtension` and by creating and configuring the `S3MockExtension` using a _builder_.
   See examples: [`S3MockExtensionProgrammaticTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk1/S3MockExtensionProgrammaticTest.java) (for SDKv1)
   or [`S3MockExtensionProgrammaticTest`](testsupport/junit5/src/test/kotlin/com/adobe/testing/s3mock/junit5/sdk2/S3MockExtensionProgrammaticTest.kt) (for SDKv2)

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

The example [`S3MockListenerXMLConfigurationTest`](testsupport/testng/src/test/kotlin/com/adobe/testing/s3mock/testng/S3MockListenerXmlConfigurationTest.kt)
demonstrates the usage of the `S3MockListener`, which can be configured as shown in [`testng.xml`](testsupport/testng/src/test/resources/testng.xml).
The listener bootstraps the S3Mock application before TestNG execution starts and shuts down the application just before the execution terminates.
Please refer to [`IExecutionListener`](https://github.com/testng-team/testng/blob/master/testng-core-api/src/main/java/org/testng/IExecutionListener.java)
in the TestNG API.

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

## File System Structure

S3Mock stores Buckets, Objects, Parts and other data on the disk.  
This lets users inspect the stored data while the S3Mock is running.  
If the environment variable `COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT` is set to `true`, this data will not be deleted when S3Mock is shut down.

| :exclamation: FYI                                                                                                                                                                                                                                   |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| While it _may_ be possible to start S3Mock on a root folder from a previous run and have all data available through the S3 API, the structure and contents of the files are not considered Public API, and are subject to change in later releases. |
| Also, there are no automated test cases for this behaviour.                                                                                                                                                                                         |

### Root-Folder

S3Mock stores buckets and objects in a root-folder.

This folder is expected to be empty when S3Mock starts. See also FYI above.

```
/<root-folder>/
```

### Buckets

Buckets are stored as a folder with their name as created through the S3 API directly below the root:

```
/<root-folder>/<bucket-name>/
```

[BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) is stored in a file in the
bucket directory, serialized as JSON.  
[BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) contains the "key" -> "uuid"
dictionary for all objects uploaded to this bucket among other data.

```
/<root-folder>/<bucket-name>/bucketMetadata.json
```

### Objects

Objects are stored in folders below the bucket they were created in.
A folder is created that uses the Object's UUID assigned in
the [BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java) as a name.

```
/<root-folder>/<bucket-name>/<uuid>/
```

Object data is stored below that UUID folder.

Binary data is always stored in a file `binaryData`

```
/<root-folder>/<bucket-name>/<uuid>/binaryData
```

[Object metadata](server/src/main/java/com/adobe/testing/s3mock/store/S3ObjectMetadata.java) is serialized as JSON and
stored as `objectMetadata.json`

```
/<root-folder>/<bucket-name>/<uuid>/objectMetadata.json
```

#### Object versions

If versioning is enabled for a bucket, multiple versions of an object can be stored in S3Mock. The files will be stored
alongside each other, prefixed by the versionId.

```
/<root-folder>/<bucket-name>/<uuid>/<versionId>-binaryData
/<root-folder>/<bucket-name>/<uuid>/<versionId>-objectMetadata.json
```

### Multipart Uploads

Multipart Uploads are created in a bucket using object keys and an uploadId.  
The object is assigned a UUID within the bucket (stored
in [BucketMetadata](server/src/main/java/com/adobe/testing/s3mock/store/BucketMetadata.java)).  
The [Multipart upload metadata](server/src/main/java/com/adobe/testing/s3mock/store/MultipartUploadInfo.java) is
currently not stored on disk.

The multiparts folder is created below the bucket folder and named with the `uploadId`:

```
/<root-folder>/<bucket-name>/multiparts/<uploadId>/
```

The multiparts metadata file is created below the folder named with the `uploadId`:

```
/<root-folder>/<bucket-name>/multiparts/<uploadId>/multipartMetadata.json
```

Each part is stored in the parts folder with the `partNo` as name and `.part` as a suffix.

```
/<root-folder>/<bucket-name>/multiparts/<uploadId>/<partNo>.part
```

Once the MultipartUpload is completed, the `uploadId` folder is deleted.

## Build & Run

### Build

#### With Docker

To build this project, you need Docker, JDK 17 or higher, and Maven:

```shell
./mvnw clean install
```

#### Without Docker

If you want to skip the Docker build, pass the optional parameter "skipDocker":

```shell
./mvnw clean install -DskipDocker
```

### Run

You can run the S3Mock from the sources by either of the following methods:

#### Spring Application

Run or Debug the class `com.adobe.testing.s3mock.S3MockApplication` in the IDE.

#### Docker

Use Docker:

```shell
./mvnw clean package -pl server -am -DskipTests
docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock:latest
```

#### Docker Maven plugin

Use the Docker Maven plugin:

```shell
./mvnw clean package docker:start -pl server -am -DskipTests -Ddocker.follow -Dit.s3mock.port_http=9090 -Dit.s3mock.port_https=9191
```
(stop with `ctrl-c`)

### Run integration tests

Once the application is started using default ports, you can execute the `*IT` tests from your IDE.

### Java

This repo is built with Java 17, output is _currently_ bytecode compatible with Java 17.

### Kotlin

The [Integration Tests](integration-tests) and unit tests are built in Kotlin.

## Governance model

The project owner and leads make all final decisions. See the `developers` section in the [pom.xml](pom.xml) for a list
of leads.

## Vulnerability reports

S3Mock uses GitHub actions to produce an SBOM and to check dependencies for vulnerabilities. All vulnerabilities are
evaluated and fixed if possible.
Vulnerabilities may also be reported through the GitHub issue tracker.

## Security

S3Mock is not intended to be used in production environments. It is a mock server meant to be used in
development and testing environments only. It does not implement all security features of AWS S3 and should not be used
as a replacement for AWS S3 in production.
It is implemented using [Spring Boot](https://github.com/spring-projects/spring-boot), which is a Java framework that is
designed to be secure by default.

## Contributing

Contributions are welcome! Read the [Contributing Guide](./.github/CONTRIBUTING.md) for more information.

## Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.

## Powered by
[![IntelliJ IDEA logo.](https://resources.jetbrains.com/storage/products/company/brand/logos/IntelliJ_IDEA.svg)](https://jb.gg/OpenSourceSupport)

## Star History
[![Star History Chart](https://api.star-history.com/svg?repos=adobe/S3Mock&type=Date)](https://www.star-history.com/#adobe/S3Mock&Date)
