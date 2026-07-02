/*
 *  Copyright 2017-2026 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.testing.s3mock.its

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.springframework.http.MediaType
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

/**
 * Verifies raw HTTP results for those methods where S3 Client from AWS SDK does not return anything
 * resp. where it's not possible to verify e.g. status codes.
 */
internal class PlainHttpIT : S3TestBase() {
  private val httpClient: HttpClient = createHttpClient()
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun putObjectReturns200(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val byteArray = UUID.randomUUID().toString().toByteArray()
    val putObject =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/testObjectName"))
        .PUT(HttpRequest.BodyPublishers.ofByteArray(byteArray))
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM_VALUE)
        .build()

    val response = httpClient.send(putObject, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testGetObject_withAcceptHeader(testInfo: TestInfo) {
    val (targetBucket, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    val getObject =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/$UPLOAD_FILE_NAME"))
        .GET()
        .header("Accept", MediaType.TEXT_PLAIN_VALUE)
        .build()

    val response = httpClient.send(getObject, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  @Test
  @S3VerifiedFailure(
    year = 2023,
    reason = "No credentials sent in plain HTTP request",
  )
  fun putHeadObject_withUserMetadata(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val byteArray = UUID.randomUUID().toString().toByteArray()
    val amzMetaHeaderKey = "x-amz-meta-my-key"
    val amzMetaHeaderValue = "MY_DATA"
    val putObject =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/testObjectName"))
        .PUT(HttpRequest.BodyPublishers.ofByteArray(byteArray))
        .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM_VALUE)
        .header(amzMetaHeaderKey, amzMetaHeaderValue)
        .build()

    val putResponse = httpClient.send(putObject, HttpResponse.BodyHandlers.discarding())
    assertThat(putResponse.statusCode()).isEqualTo(200)

    val headObject =
      HttpRequest
        .newBuilder(URI.create("http://$host:$httpPort/$targetBucket/testObjectName"))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .build()
    val headResponse = httpClient.send(headObject, HttpResponse.BodyHandlers.discarding())
    assertThat(headResponse.statusCode()).isEqualTo(200)
    assertThat(headResponse.headers().firstValue(amzMetaHeaderKey).orElse(null)).isEqualTo(amzMetaHeaderValue)
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "No credentials sent in plain HTTP request",
  )
  fun createBucketWithDisallowedName() {
    val putRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$INVALID_BUCKET_NAME"))
        .PUT(HttpRequest.BodyPublishers.noBody())
        .build()
    val response = httpClient.send(putRequest, HttpResponse.BodyHandlers.ofByteArray())
    assertThat(response.statusCode()).isEqualTo(400)
    assertThat(response.body().toString(Charsets.UTF_8)).isEqualTo(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidBucketName</Code>" +
        "<Message>The specified bucket is not valid.</Message></Error>",
    )
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun putObjectEncryptedWithAbsentKeyRef(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    val putRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/testObjectName"))
        .PUT(HttpRequest.BodyPublishers.ofByteArray(UUID.randomUUID().toString().toByteArray()))
        .header("x-amz-server-side-encryption", "aws:kms")
        .build()
    val response = httpClient.send(putRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun listWithPrefixAndMissingSlash(testInfo: TestInfo) {
    val (targetBucket, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    val getRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket?prefix=${UPLOAD_FILE_NAME}%2F&encoding-type=url"))
        .GET()
        .build()
    val response = httpClient.send(getRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun objectUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    val getRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket"))
        .GET()
        .build()
    assertApplicationXmlContentType(getRequest)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testCorsHeaders_GET_PUT_HEAD(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    arrayOf("GET", "PUT", "HEAD").forEach { method ->
      val httpOptions =
        HttpRequest
          .newBuilder(URI.create("$serviceEndpoint/$targetBucket"))
          .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
          .header("Origin", "http://someurl.com")
          .header("Access-Control-Request-Method", method)
          .header("Access-Control-Request-Headers", "Content-Type, x-requested-with")
          .build()

      val response = httpClient.send(httpOptions, HttpResponse.BodyHandlers.discarding())
      assertThat(response.statusCode()).isEqualTo(200)
      assertThat(response.headers().firstValue("Access-Control-Allow-Origin").orElse(null))
        .isEqualTo("http://someurl.com")
      assertThat(response.headers().firstValue("Access-Control-Allow-Methods").orElse(null))
        .isEqualTo(method)
      assertThat(response.headers().firstValue("Access-Control-Allow-Headers").orElse(null))
        .isEqualTo("Content-Type, x-requested-with")
      assertThat(response.headers().firstValue("Access-Control-Allow-Credentials").orElse(null))
        .isEqualTo("true")
      assertThat(response.headers().firstValue("Allow").orElse(null)).contains(method)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun testCorsHeaders_POST(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    arrayOf("POST").forEach { method ->
      val httpOptions =
        HttpRequest
          .newBuilder(URI.create("$serviceEndpoint/$targetBucket?delete"))
          .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
          .header("Origin", "http://someurl.com")
          .header("Access-Control-Request-Method", method)
          .header("Access-Control-Request-Headers", "Content-Type, x-requested-with")
          .build()

      val response = httpClient.send(httpOptions, HttpResponse.BodyHandlers.discarding())
      assertThat(response.statusCode()).isEqualTo(200)
      // for POST requests, Access-Control-Allow-Origin is always returned as "*"
      assertThat(response.headers().firstValue("Access-Control-Allow-Origin").orElse(null))
        .isEqualTo("*")
      assertThat(response.headers().firstValue("Access-Control-Allow-Methods").orElse(null))
        .isEqualTo(method)
      assertThat(response.headers().firstValue("Access-Control-Allow-Headers").orElse(null))
        .isEqualTo("Content-Type, x-requested-with")
      // for POST requests, Access-Control-Allow-Credentials is never returned.
      // assertThat(response.headers().firstValue("Access-Control-Allow-Credentials").orElse(null)).isEqualTo("true")
      assertThat(response.headers().firstValue("Allow").orElse(null)).contains(method)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "No credentials sent in plain HTTP request",
  )
  fun listBucketsUsesApplicationXmlContentType(testInfo: TestInfo) {
    givenBucket(testInfo)
    val getRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint$SLASH"))
        .GET()
        .build()
    assertApplicationXmlContentType(getRequest)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun batchDeleteUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    val postRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket?delete"))
        .POST(
          HttpRequest.BodyPublishers.ofString(
            """<?xml version="1.0" encoding="UTF-8"?><Delete>
          <Object><Key>myFile-1</Key></Object>
          <Object><Key>myFile-2</Key></Object>
          </Delete>""",
          ),
        ).header("Content-Type", "application/xml")
        .build()
    assertApplicationXmlContentType(postRequest)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun completeMultipartUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val initiateMultipartUploadResult =
      s3Client
        .createMultipartUpload {
          it.bucket(targetBucket)
          it.key(UPLOAD_FILE_NAME)
        }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(initiateMultipartUploadResult.bucket())
          it.key(initiateMultipartUploadResult.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    val postRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/$UPLOAD_FILE_NAME?uploadId=$uploadId"))
        .POST(
          HttpRequest.BodyPublishers.ofString(
            """<?xml version="1.0" encoding="UTF-8"?>
          <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Part>
          <ETag>${uploadPartResult.eTag()}</ETag>
          <PartNumber>1</PartNumber>
          </Part>
          </CompleteMultipartUpload>""",
          ),
        ).header("Content-Type", "application/xml")
        .build()
    assertApplicationXmlContentType(postRequest)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun putObjectWithSpecialCharactersInTheName(testInfo: TestInfo) {
    val fileNameWithSpecialCharacters = (
      "file=name\$Dollar;Semicolon" +
        "&Ampersand@At:Colon     Space,Comma?Question-mark"
    )
    val targetBucket = givenBucket(testInfo)

    val putRequest =
      HttpRequest
        .newBuilder(
          URI.create(
            "$serviceEndpoint/$targetBucket/${SdkHttpUtils.urlEncodeIgnoreSlashes(fileNameWithSpecialCharacters)}",
          ),
        ).PUT(HttpRequest.BodyPublishers.ofByteArray(UUID.randomUUID().toString().toByteArray()))
        .build()
    val response = httpClient.send(putRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)

    assertThat(
      s3Client
        .listObjects {
          it.bucket(targetBucket)
          it.prefix(fileNameWithSpecialCharacters)
        }.contents()[0]
        .key(),
    ).isEqualTo(fileNameWithSpecialCharacters)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun deleteNonExistingObjectReturns204(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    val deleteRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/${UUID.randomUUID()}"))
        .DELETE()
        .build()
    val response = httpClient.send(deleteRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(204)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun batchDeleteObjects(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    val postRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket?delete"))
        .POST(
          HttpRequest.BodyPublishers.ofString(
            """<?xml version="1.0" encoding="UTF-8"?>
           <Delete>
           <Object><Key>myFile-1</Key></Object>
           <Object><Key>myFile-2</Key></Object>
           </Delete>""",
          ),
        ).header("Content-Type", "application/xml")
        .build()
    val response = httpClient.send(postRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  @Test
  @S3VerifiedFailure(
    year = 2022,
    reason = "No credentials sent in plain HTTP request",
  )
  fun headObjectWithUnknownContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val contentAsBytes = ByteArray(0)
    val blankContentTypeFilename = UUID.randomUUID().toString()
    s3Client.putObject(
      {
        it.bucket(targetBucket)
        it.key(blankContentTypeFilename)
        it.contentType(UUID.randomUUID().toString())
        it.contentLength(contentAsBytes.size.toLong())
      },
      RequestBody.fromBytes(contentAsBytes),
    )

    val headRequest =
      HttpRequest
        .newBuilder(URI.create("$serviceEndpoint/$targetBucket/$blankContentTypeFilename"))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .build()
    val response = httpClient.send(headRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(200)
  }

  private fun assertApplicationXmlContentType(request: HttpRequest) {
    val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())
    assertThat(response.headers().firstValue("Content-Type").orElse(null)).isEqualTo("application/xml")
  }

  companion object {
    private const val SLASH = "/"
    const val INVALID_BUCKET_NAME = "invalidBucketName"
  }
}
