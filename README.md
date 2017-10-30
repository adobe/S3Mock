[![Latest Version](https://img.shields.io/maven-central/v/com.adobe.testing/s3mock.svg?maxAge=3600&label=Latest%20Release)](https://search.maven.org/#search%7Cga%7C1%7Cg%3Acom.adobe.testing%20a%3As3mock)
[![Docker Hub](https://img.shields.io/badge/docker-latest-blue.svg)](https://hub.docker.com/r/adobe/s3mock/)
[![Build Status](https://travis-ci.org/adobe/S3Mock.svg?branch=master)](https://travis-ci.org/adobe/S3Mock)


S3Mock
======

### Introduction

`S3Mock` is a lightweight server that implements parts of the Amazon S3 API.
It has been created to support hermetic testing and reduces the infrastructure dependencies while testing.

The mock server can be started as a Docker container or using a JUnit `Rule`.

Similiar projects are e.g.:

 - [Fake S3](https://github.com/jubos/fake-s3)
 - [Mock S3](https://github.com/jserver/mock-s3)
 - [S3 Proxy](https://github.com/andrewgaul/s3proxy)

### Build & Run

Run the s3 mock (adjust the port numbers as desired):

    mvn clean package docker:start -Ddocker.follow -Dit.s3mock.port_http=9090 -Dit.s3mock.port_https=9191

    ... stop with ctrl-c.

### Docker Support

Build and create the docker container:

     mvn clean package

Run the created docker container:

    docker run -p 9090:9090 -p 9191:9191 -t adobe/s3mock

    ... stop with ctrl-c.

### Configuration

The mock can be configured with the following environment parameters:

- `validKmsKeys`: list of KMS Key-Refs that are to be treated as *valid*.
- `initialBuckets`: list of names for buckets that will be available initially.

### Usage

#### With the Docker plugin

Our integration tests are using the Amazon S3 Client to verify the server functionality against the S3Mock. During the Maven build, the Docker image is started using the [docker-maven-plugin](https://dmp.fabric8.io/) and the corresponding ports are passed to the JUnit test through the `maven-failsafe-plugin`. See [`AmazonClientUploadIT`](src/test/java/com/adobe/testing/s3mock/its/AmazonClientUploadIT.java) how it's used in the code.

This way, one can easily switch between calling the S3Mock or the real S3 endpoint and this doesn't add any additional Java dependencies to the project.

#### With the JUnit Rule

The test [`S3MockRuleTest`](src/test/java/com/adobe/testing/s3mock/S3MockRuleTest.java) demonstrates the usage of the `S3MockRule`. This requires the Maven artifact to be added to the project in `test` scope:

```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock</artifactId>
  <version>...</version>
  <scope>test</scope>
<dependency>
```

### Contribution

Contributions are welcome. Please fork the project and create for your finished feature a pull request.

Our [Adobe Code of Conduct](CODE_OF_CONDUCT.md) applies.

### Licensing

This project is licensed under the Apache V2 License. See [LICENSE](LICENSE) for more information.
