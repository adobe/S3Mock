/*
 *  Copyright 2017-2025 Adobe.
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

import org.apache.http.HttpHeaders
import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpHead
import org.apache.http.client.methods.HttpOptions
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.springframework.http.MediaType
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.File
import java.io.InputStreamReader
import java.util.UUID
import java.util.stream.Collectors

/**
 * Verifies raw HTTP results for those methods where S3 Client from AWS SDK does not return anything
 * resp. where it's not possible to verify e.g. status codes.
 */
internal class PlainHttpIT : S3TestBase() {
  private val httpClient: CloseableHttpClient = createHttpClient()
  private val s3Client: S3Client = createS3Client()

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectReturns200(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val byteArray = UUID.randomUUID().toString().toByteArray()
    val putObject = HttpPut("$serviceEndpoint/$targetBucket/testObjectName").apply {
      this.entity = ByteArrayEntity(byteArray)
      this.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
    }

    httpClient.execute(putObject).use {
      assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun testGetObject_withAcceptHeader(testInfo: TestInfo) {
    val (targetBucket, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    val getObject = HttpGet("$serviceEndpoint/$targetBucket/$UPLOAD_FILE_NAME").apply {
      this.addHeader(HttpHeaders.ACCEPT, MediaType.TEXT_PLAIN_VALUE)
    }

    httpClient.execute(getObject).use {
      assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2023,
    reason = "No credentials sent in plain HTTP request")
  fun putHeadObject_withUserMetadata(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val byteArray = UUID.randomUUID().toString().toByteArray()
    val amzMetaHeaderKey = "x-amz-meta-my-key"
    val amzMetaHeaderValue = "MY_DATA"
    val putObject = HttpPut("$serviceEndpoint/$targetBucket/testObjectName").apply {
      this.entity = ByteArrayEntity(byteArray)
      this.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
      this.addHeader(amzMetaHeaderKey, amzMetaHeaderValue)
    }

    httpClient.execute(putObject).use {
      assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    val headObject = HttpHead("/$targetBucket/testObjectName")
    httpClient.execute(
      HttpHost(
        host, httpPort
      ), headObject
    ).use {
      assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      assertThat(it.getFirstHeader(amzMetaHeaderKey).name).isEqualTo(amzMetaHeaderKey)
      assertThat(it.getFirstHeader(amzMetaHeaderKey).value).isEqualTo(amzMetaHeaderValue)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun createBucketWithDisallowedName() {
    HttpPut("$serviceEndpoint/$INVALID_BUCKET_NAME").also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_BAD_REQUEST)
        assertThat(
          InputStreamReader(response.entity.content)
            .readLines()
            .stream()
            .collect(Collectors.joining()))
          .isEqualTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidBucketName</Code>" +
            "<Message>The specified bucket is not valid.</Message></Error>")
      }
    }

  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectEncryptedWithAbsentKeyRef(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    HttpPut("$serviceEndpoint/$targetBucket/testObjectName").apply {
      this.addHeader("x-amz-server-side-encryption", "aws:kms")
      this.entity = ByteArrayEntity(UUID.randomUUID().toString().toByteArray())
    }.also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun listWithPrefixAndMissingSlash(testInfo: TestInfo) {
    val (targetBucket, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    HttpGet("$serviceEndpoint/$targetBucket?prefix=${UPLOAD_FILE_NAME}%2F&encoding-type=url").also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }

  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun objectUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    HttpGet("$serviceEndpoint/$targetBucket").also {
      assertApplicationXmlContentType(it)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun testCorsHeaders_GET_PUT_HEAD(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    arrayOf("GET", "PUT", "HEAD").forEach { method ->
      val httpOptions = HttpOptions("$serviceEndpoint/$targetBucket").apply {
        this.setHeader(BasicHeader("Origin", "http://someurl.com"))
        this.setHeader(BasicHeader("Access-Control-Request-Method", method))
        this.setHeader(BasicHeader("Access-Control-Request-Headers", "Content-Type, x-requested-with"))
      }

      httpClient.execute(httpOptions).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        assertThat(it.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("http://someurl.com")
        assertThat(it.getFirstHeader("Access-Control-Allow-Methods").value).isEqualTo(method)
        assertThat(it.getFirstHeader("Access-Control-Allow-Headers").value)
          .isEqualTo("Content-Type, x-requested-with")
        assertThat(it.getFirstHeader("Access-Control-Allow-Credentials").value).isEqualTo("true")
        assertThat(it.getFirstHeader("Allow").value).contains(method)
      }
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun testCorsHeaders_POST(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    arrayOf("POST").forEach { method ->
      val httpOptions = HttpOptions("$serviceEndpoint/$targetBucket?delete").apply {
        this.setHeader(BasicHeader("Origin", "http://someurl.com"))
        this.setHeader(BasicHeader("Access-Control-Request-Method", method))
        this.setHeader(BasicHeader("Access-Control-Request-Headers", "Content-Type, x-requested-with"))
      }

      httpClient.execute(httpOptions).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        //for POST requests, Access-Control-Allow-Origin is always returned as "*"
        assertThat(it.getFirstHeader("Access-Control-Allow-Origin").value).isEqualTo("*")
        assertThat(it.getFirstHeader("Access-Control-Allow-Methods").value).isEqualTo(method)
        assertThat(it.getFirstHeader("Access-Control-Allow-Headers").value)
          .isEqualTo("Content-Type, x-requested-with")
        //for POST requests, Access-Control-Allow-Credentials is never returned.
        //assertThat(it.getFirstHeader("Access-Control-Allow-Credentials").value).isEqualTo("true")
        assertThat(it.getFirstHeader("Allow").value).contains(method)
      }
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun listBucketsUsesApplicationXmlContentType(testInfo: TestInfo) {
    givenBucket(testInfo)
    HttpGet("$serviceEndpoint$SLASH").also {
      assertApplicationXmlContentType(it)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun batchDeleteUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    HttpPost("$serviceEndpoint/$targetBucket?delete").apply {
      this.entity = StringEntity(
        """<?xml version="1.0" encoding="UTF-8"?><Delete>
          <Object><Key>myFile-1</Key></Object>
          <Object><Key>myFile-2</Key></Object>
          </Delete>""".trimMargin(),
        ContentType.APPLICATION_XML
      )
    }.also {
      assertApplicationXmlContentType(it)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun completeMultipartUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .createMultipartUpload {
        it.bucket(targetBucket)
        it.key(UPLOAD_FILE_NAME)
      }
    val uploadId = initiateMultipartUploadResult.uploadId()

    val uploadPartResult = s3Client.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(uploadFile.length())
      },
      RequestBody.fromFile(uploadFile)
    )


    HttpPost("$serviceEndpoint/$targetBucket/$UPLOAD_FILE_NAME?uploadId=$uploadId").apply {
      this.entity = StringEntity(
        """<?xml version="1.0" encoding="UTF-8"?>
          <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Part>
          <ETag>${uploadPartResult.eTag()}</ETag>
          <PartNumber>1</PartNumber>
          </Part>
          </CompleteMultipartUpload>""".trimMargin(),
        ContentType.APPLICATION_XML
      )
    }.also {
      assertApplicationXmlContentType(it)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectWithSpecialCharactersInTheName(testInfo: TestInfo) {
    val fileNameWithSpecialCharacters = ("file=name\$Dollar;Semicolon"
      + "&Ampersand@At:Colon     Space,Comma?Question-mark")
    val targetBucket = givenBucket(testInfo)

    HttpPut(
      "$serviceEndpoint/$targetBucket/${SdkHttpUtils.urlEncodeIgnoreSlashes(fileNameWithSpecialCharacters)}"
    ).apply {
      this.entity = ByteArrayEntity(UUID.randomUUID().toString().toByteArray())
    }.also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }

    assertThat(
      s3Client.listObjects {
        it.bucket(targetBucket)
        it.prefix(fileNameWithSpecialCharacters)
      }.contents()[0].key()
    ).isEqualTo(fileNameWithSpecialCharacters)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun deleteNonExistingObjectReturns204(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    HttpDelete("$serviceEndpoint/$targetBucket/${UUID.randomUUID()}").also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_NO_CONTENT)
      }
    }

  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun batchDeleteObjects(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)

    HttpPost("$serviceEndpoint/$targetBucket?delete").apply {
      this.entity = StringEntity(
        """<?xml version="1.0" encoding="UTF-8"?>
           <Delete>
           <Object><Key>myFile-1</Key></Object>
           <Object><Key>myFile-2</Key></Object>
           </Delete>""".trimMargin(),
        ContentType.APPLICATION_XML
      )
    }.also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }

  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun headObjectWithUnknownContentType(testInfo: TestInfo) {
    val targetBucket = givenBucket(testInfo)
    val contentAsBytes = ByteArray(0)
    val blankContentTypeFilename = UUID.randomUUID().toString()
    s3Client.putObject({
        it.bucket(targetBucket)
        it.key(blankContentTypeFilename)
        it.contentType(UUID.randomUUID().toString())
        it.contentLength(contentAsBytes.size.toLong())
      },
      RequestBody.fromBytes(contentAsBytes)
    )

    HttpHead("$serviceEndpoint/$targetBucket/$blankContentTypeFilename").also {
      httpClient.execute(it).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }
  }

  private fun assertApplicationXmlContentType(httpRequestBase: HttpRequestBase) {
    httpClient.execute(httpRequestBase).use {
      assertThat(it.getFirstHeader(HttpHeaders.CONTENT_TYPE).value).isEqualTo("application/xml")
    }
  }

  companion object {
    private const val SLASH = "/"
    const val INVALID_BUCKET_NAME = "invalidBucketName"
  }
}
