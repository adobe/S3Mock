/*
 *  Copyright 2017-2023 Adobe.
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

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.UploadPartRequest
import org.apache.http.HttpHeaders
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpHead
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.springframework.http.MediaType
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStreamReader
import java.util.UUID
import java.util.stream.Collectors

/**
 * Verifies raw HTTP results for those methods where S3 Client from AWS SDK does not return anything
 * resp. where it's not possible to verify e.g. status codes.
 */
internal class PlainHttpIT : S3TestBase() {
  private lateinit var httpClient: CloseableHttpClient

  @BeforeEach
  fun setupHttpClient() {
    httpClient = HttpClients.createDefault()
  }

  @AfterEach
  fun shutdownHttpClient() {
    httpClient.close()
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectReturns200(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val putObject = HttpPut("/$targetBucket/testObjectName")
    val byteArray = UUID.randomUUID().toString().toByteArray()
    putObject.entity = ByteArrayEntity(byteArray)
    putObject.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
  }

  @Test
  @S3VerifiedFailure(year = 2023,
    reason = "No credentials sent in plain HTTP request")
  fun putHeadObject_withUserMetadata(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val putObject = HttpPut("/$targetBucket/testObjectName")
    val byteArray = UUID.randomUUID().toString().toByteArray()
    putObject.entity = ByteArrayEntity(byteArray)
    putObject.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
    val amzMetaHeaderKey = "X-Amz-Meta-My-Key"
    val amzMetaHeaderValue = "MY_DATA"
    putObject.addHeader(amzMetaHeaderKey, amzMetaHeaderValue)
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)

    val headObject = HttpHead("/$targetBucket/testObjectName")
    val headObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), headObject
    )
    assertThat(headObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    assertThat(headObjectResponse.getFirstHeader(amzMetaHeaderKey).name).isEqualTo(amzMetaHeaderKey)
    assertThat(headObjectResponse.getFirstHeader(amzMetaHeaderKey).value).isEqualTo(amzMetaHeaderValue)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun createBucketWithDisallowedName() {
    val putObject = HttpPut("/$INVALID_BUCKET_NAME")
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_BAD_REQUEST)
    assertThat(
      InputStreamReader(putObjectResponse.entity.content)
        .readLines()
        .stream()
        .collect(Collectors.joining()))
      .isEqualTo("<Error><Code>InvalidBucketName</Code>" +
        "<Message>The specified bucket is not valid.</Message><Resource/><RequestId/></Error>")
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectEncryptedWithAbsentKeyRef(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val putObject = HttpPut("/$targetBucket/testObjectName")
    putObject.addHeader("x-amz-server-side-encryption", "aws:kms")
    putObject.entity =
      ByteArrayEntity(UUID.randomUUID().toString().toByteArray())
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun listWithPrefixAndMissingSlash(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    s3Client.putObject(targetBucket, "prefix", "Test")
    val getObject = HttpGet("/$targetBucket?prefix=prefix%2F&encoding-type=url")
    val getObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), getObject
    )
    assertThat(getObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun objectUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val getObject = HttpGet("/$targetBucket")
    assertApplicationXmlContentType(getObject)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun listBucketsUsesApplicationXmlContentType(testInfo: TestInfo) {
    givenBucketV2(testInfo)
    val listBuckets = HttpGet(SLASH)
    assertApplicationXmlContentType(listBuckets)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun batchDeleteUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val postObject = HttpPost("/$targetBucket?delete")
    postObject.entity = StringEntity(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>"
        + "<Object><Key>myFile-1</Key></Object>"
        + "<Object><Key>myFile-2</Key></Object>"
        + "</Delete>", ContentType.APPLICATION_XML
    )
    assertApplicationXmlContentType(postObject)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun completeMultipartUsesApplicationXmlContentType(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .initiateMultipartUpload(
        InitiateMultipartUploadRequest(targetBucket, UPLOAD_FILE_NAME)
      )
    val uploadId = initiateMultipartUploadResult.uploadId
    val uploadPartResult = s3Client.uploadPart(
      UploadPartRequest()
        .withBucketName(initiateMultipartUploadResult.bucketName)
        .withKey(initiateMultipartUploadResult.key)
        .withUploadId(uploadId)
        .withFile(uploadFile)
        .withFileOffset(0)
        .withPartNumber(1)
        .withPartSize(uploadFile.length())
        .withLastPart(true)
    )
    val postObject = HttpPost("/$targetBucket/$UPLOAD_FILE_NAME?uploadId=$uploadId")
    postObject.entity = StringEntity(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        + "<Part>"
        + "<ETag>" + uploadPartResult.partETag.eTag + "</ETag>"
        + "<PartNumber>1</PartNumber>"
        + "</Part>"
        + "</CompleteMultipartUpload>", ContentType.APPLICATION_XML
    )
    assertApplicationXmlContentType(postObject)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun putObjectWithSpecialCharactersInTheName(testInfo: TestInfo) {
    val fileNameWithSpecialCharacters = ("file=name\$Dollar;Semicolon"
      + "&Ampersand@At:Colon     Space,Comma?Question-mark")
    val targetBucket = givenBucketV2(testInfo)
    val putObject = HttpPut(
      "/$targetBucket/${SdkHttpUtils.urlEncodeIgnoreSlashes(fileNameWithSpecialCharacters)}"
    )
    putObject.entity =
      ByteArrayEntity(UUID.randomUUID().toString().toByteArray())
    val putObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), putObject
    )
    assertThat(putObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    assertThat(
      s3Client
        .listObjects(targetBucket)
        .objectSummaries[0]
        .key
    ).isEqualTo(fileNameWithSpecialCharacters)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun deleteNonExistingObjectReturns204(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val deleteObject =
      HttpDelete("/$targetBucket/${UUID.randomUUID()}")
    val deleteObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), deleteObject
    )
    assertThat(deleteObjectResponse.statusLine.statusCode)
      .isEqualTo(HttpStatus.SC_NO_CONTENT)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun batchDeleteObjects(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val postObject = HttpPost("/$targetBucket?delete")
    postObject.entity = StringEntity(
      "<?xml version=\"1.0\" "
        + "encoding=\"UTF-8\"?><Delete><Object><Key>myFile-1</Key></Object><Object><Key>myFile-2"
        + "</Key></Object></Delete>", ContentType.APPLICATION_XML
    )
    val response = httpClient.execute(HttpHost(host, httpPort), postObject)
    assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
  }

  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No credentials sent in plain HTTP request")
  fun headObjectWithUnknownContentType(testInfo: TestInfo) {
    val targetBucket = givenBucketV2(testInfo)
    val contentAsBytes = ByteArray(0)
    val md = ObjectMetadata()
    md.contentLength = contentAsBytes.size.toLong()
    md.contentType = UUID.randomUUID().toString()
    val blankContentTypeFilename = UUID.randomUUID().toString()
    s3Client.putObject(
      targetBucket, blankContentTypeFilename,
      ByteArrayInputStream(contentAsBytes), md
    )
    val headObject = HttpHead("/$targetBucket/$blankContentTypeFilename")
    val headObjectResponse: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), headObject
    )
    assertThat(headObjectResponse.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
  }

  private fun assertApplicationXmlContentType(httpRequestBase: HttpRequestBase) {
    val response: HttpResponse = httpClient.execute(
      HttpHost(
        host, httpPort
      ), httpRequestBase
    )
    assertThat(
      response.getFirstHeader(HttpHeaders.CONTENT_TYPE).value
    ).isEqualTo("application/xml")
  }

  companion object {
    private const val SLASH = "/"
    const val INVALID_BUCKET_NAME = "invalidBucketName"
  }
}
