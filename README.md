[![Latest Version](https://img.shields.io/maven-central/v/com.adobe.testing/s3mock.svg?maxAge=3600&label=Latest%20Release)](https://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.adobe.testing%20a%3As3mock)
[![Docker Hub](https://img.shields.io/badge/docker-latest-blue.svg)](https://hub.docker.com/r/adobe/s3mock/)
![Maven Build](https://github.com/adobe/S3Mock/workflows/Maven%20Build/badge.svg)
[![JAVA](https://img.shields.io/badge/MADE%20with-JAVA-RED.svg)](#JAVA)
[![Docker Pulls](https://img.shields.io/docker/pulls/adobe/s3mock)](https://hub.docker.com/r/adobe/s3mock)
[![GitHub stars](https://img.shields.io/github/stars/adobe/S3Mock.svg?style=social&label=Star&maxAge=2592000)](https://github.com/adobe/S3Mock/stargazers/)



S3Mock
======

## Introduction

`S3Mock` is a lightweight server that implements parts of the Amazon S3 API.
It has been created to support hermetic testing and reduces the infrastructure dependencies while testing.

The mock server can be started as a standalone *Docker* container, through the *Testcontainers*, *JUnit4*, *JUnit5* and *TestNG* support, or programmatically.

Similar projects are e.g.:

 - [Fake S3](https://github.com/jubos/fake-s3)
 - [Mock S3](https://github.com/jserver/mock-s3)
 - [S3 Proxy](https://github.com/andrewgaul/s3proxy)

## Storage
S3Mock stores Buckets, Objects, Multiparts and other data on disk.  
This lets users inspect the stored data while the S3Mock is running.  
If the config property `retainFilesOnExit` is set to `true`, data will not be deleted when S3Mock is shut down.

### Root
S3Mock stores buckets and objects in the root-folder.
```
/<root-folder>/
```

### Buckets
Buckets are stored as a folder with their name directly below the root:
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

The parts folder is created below the object UUID folder:
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

### Using the Docker image

Starting on the command-line:

    docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock

The port `9090` is for HTTP, port `9191` is for HTTPS.

### Starting with the Docker Maven Plugin

Our [integration tests](integration-tests) are using the Amazon S3 Client to verify the server functionality against the S3Mock. During the Maven build, the Docker image is started using the [docker-maven-plugin](https://dmp.fabric8.io/) and the corresponding ports are passed to the JUnit test through the `maven-failsafe-plugin`. See [`AmazonClientUploadIT`](integration-tests/src/test/java/com/adobe/testing/s3mock/its/AmazonClientUploadIT.java) how it's used in the code.

This way, one can easily switch between calling the S3Mock or the real S3 endpoint and this doesn't add any additional Java dependencies to the project.

### Using the Docker container with Testcontainers

The [`S3MockContainer`](testsupport/testcontainers/src/main/java/com/adobe/testing/s3mock/testcontainers/S3MockContainer.java) is a Testcontainer implementation that comes pre-configured exposing HTTP and HTTPS ports. Environment variables can be set on startup.

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

### Using the JUnit4 Rule

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

### Using the JUnit5 Extension

The `S3MockExtension` can currently be used in two ways:

1. Declaratively using `@ExtendWith(S3MockExtension.class)` and by injecting a properly configured instance of `AmazonS3` client and/or the started `S3MockApplication` to the tests.
See examples: [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk1/S3MockExtensionDeclarativeTest.java)  (for SDKv1)
or [`S3MockExtensionDeclarativeTest`](testsupport/junit5/src/test/java/com/adobe/testing/s3mock/junit5/sdk2/S3MockExtensionDeclarativeTest.java) (for SDKv2)

1. Programmatically using `@RegisterExtension` and by creating and configuring the `S3MockExtension` using a _builder_.
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

### Using the TestNG Listener

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

### Starting Programmatically

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

## Contributing

Contributions are welcomed! Read the [Contributing Guide](./.github/CONTRIBUTING.md) for more information.

## Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.
