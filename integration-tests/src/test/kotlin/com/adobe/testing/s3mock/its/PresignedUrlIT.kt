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

import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult
import com.adobe.testing.s3mock.util.DigestUtil
import org.apache.http.HttpHeaders
import org.apache.http.HttpHeaders.CONTENT_TYPE
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.FileEntity
import org.apache.http.entity.StringEntity
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import tel.schich.awss3postobjectpresigner.S3PostObjectPresigner
import tel.schich.awss3postobjectpresigner.S3PostObjectRequest
import java.time.Duration
import java.time.Instant

internal class PresignedUrlIT : S3TestBase() {
  private val httpClient: CloseableHttpClient = createHttpClient()
  private val s3Client: S3Client = createS3Client()
  private val s3Presigner: S3Presigner = createS3Presigner()
  private val s3PostObjectPresigner: S3PostObjectPresigner = createS3PostObjectPresigner()

  @Test
  @S3VerifiedFailure(year = 2025,
    reason = "S3PostObjectPresigner does not create working presigned URLs.")
  fun testPresignedUrl_postObject_largeFile(testInfo: TestInfo) {
    val key = randomName
    val bucketName = givenBucket(testInfo)

    val presignedUrlString = s3PostObjectPresigner.presignPost(
      S3PostObjectRequest
        .builder()
        .bucket(bucketName)
        .expiration(Duration.ofMinutes(1L))
        .build()
    ).uri().toString()

    assertThat(presignedUrlString).isNotBlank()

    val randomMBytes = randomMBytes(20)
    val expectedEtag = randomMBytes.inputStream().use { "\"${DigestUtil.hexDigest(it)}\"" }
    HttpPost(presignedUrlString).apply {
        this.entity = MultipartEntityBuilder.create()
          .addTextBody("key", key)
          .addTextBody(CONTENT_TYPE, "application/octet-stream")
          //.addTextBody(X_AMZ_STORAGE_CLASS, "INTELLIGENT_TIERING")
          .addTextBody("tagging", "<Tagging><TagSet><Tag><Key>Tag Name</Key><Value>Tag Value</Value></Tag></TagSet></Tagging>")
          .addBinaryBody("file", randomMBytes.inputStream(), ContentType.APPLICATION_OCTET_STREAM, key)
          .build()
      }.also { post ->
      httpClient.execute(
        post
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        val actualEtag = it.getFirstHeader(HttpHeaders.ETAG).value
        assertThat(actualEtag).isEqualTo(expectedEtag)
      }
    }

    s3Client.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      val actualEtag = "\"${DigestUtil.hexDigest(it)}\""
      assertThat(actualEtag).isEqualTo(expectedEtag)
    }
    s3Client.getObjectTagging {
      it.bucket(bucketName)
      it.key(key)
    }.also {
      assertThat(it.tagSet()).hasSize(1)
      assertThat(it.tagSet()[0].key()).isEqualTo("Tag Name")
      assertThat(it.tagSet()[0].value()).isEqualTo("Tag Value")
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_getObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)

    val presignedUrlString = s3Presigner.presignGetObject {
      it.getObjectRequest {
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    val expectedEtag = UPLOAD_FILE.inputStream().use {
      "\"${DigestUtil.hexDigest(it)}\""
    }
    HttpGet(presignedUrlString).also { get ->
      httpClient.execute(
        get
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        val actualEtag = "\"${DigestUtil.hexDigest(it.entity.content)}\""
        assertThat(actualEtag).isEqualTo(expectedEtag)
      }
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_getObject_responseHeaderOverrides(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)

    val responseExpires = Instant.now()

    val presignedUrlString = s3Presigner.presignGetObject {
      it.getObjectRequest {
        it.bucket(bucketName)
        it.key(key)
        it.responseExpires(responseExpires)
        it.responseCacheControl("no-cache")
        it.responseContentDisposition("attachment; filename=\"$key\"")
        it.responseContentEncoding("encoding")
        it.responseContentType("application/json")
        it.responseContentLanguage("en")
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()
    val expectedEtag = UPLOAD_FILE.inputStream().use {
      "\"${DigestUtil.hexDigest(it)}\""
    }
    HttpGet(presignedUrlString).also { get ->
      httpClient.execute(
        get
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        val actualEtag = "\"${DigestUtil.hexDigest(it.entity.content)}\""
        assertThat(actualEtag).isEqualTo(expectedEtag)
        //TODO: S3 SDK serializes date as 'Sun, 20 Apr 2025 22:07:04 GMT'
        //assertThat(it.getFirstHeader(HttpHeaders.EXPIRES).value).isEqualTo(responseExpires)
        assertThat(it.getFirstHeader(HttpHeaders.CACHE_CONTROL).value).isEqualTo("no-cache")
        assertThat(it.getFirstHeader("Content-Disposition").value).isEqualTo("attachment; filename=\"$key\"")
        assertThat(it.getFirstHeader(HttpHeaders.CONTENT_ENCODING).value).isEqualTo("encoding")
        assertThat(it.getFirstHeader(CONTENT_TYPE).value).isEqualTo("application/json")
        assertThat(it.getFirstHeader(HttpHeaders.CONTENT_LANGUAGE).value).isEqualTo("en")
      }
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_getObject_range(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)

    val presignedUrlString = s3Presigner.presignGetObject {
      it.getObjectRequest{
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpGet(presignedUrlString).also { get ->
      get.setHeader(HttpHeaders.RANGE, "bytes=0-100")
      httpClient.execute(
        get
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_PARTIAL_CONTENT)
        assertThat(it.getFirstHeader(HttpHeaders.CONTENT_LENGTH).value).isEqualTo("101")
        assertThat(it.getFirstHeader(HttpHeaders.CONTENT_RANGE).value).isEqualTo("bytes 0-100/63839")
      }
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_putObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val presignedUrlString = s3Presigner.presignPutObject {
      it.putObjectRequest {
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpPut(presignedUrlString).apply {
      this.entity = FileEntity(UPLOAD_FILE)
    }.also { put ->
      httpClient.execute(
        put
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }
    val expectedEtag = UPLOAD_FILE.inputStream().use {
      "\"${DigestUtil.hexDigest(it)}\""
    }
    s3Client.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      val actualEtag = "\"${DigestUtil.hexDigest(it)}\""
      assertThat(actualEtag).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_putObject_largeFile(testInfo: TestInfo) {
    val key = randomName
    val bucketName = givenBucket(testInfo)

    val presignedUrlString = s3Presigner.presignPutObject {
      it.putObjectRequest {
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    val randomMBytes = randomMBytes(20)
    HttpPut(presignedUrlString).apply {
      this.entity = ByteArrayEntity(randomMBytes)
    }.also { put ->
      httpClient.execute(
        put
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }
    val expectedEtag = randomMBytes.inputStream().use { "\"${DigestUtil.hexDigest(it)}\"" }
    s3Client.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      val actualEtag = "\"${DigestUtil.hexDigest(it)}\""
      assertThat(actualEtag).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_createMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val presignedUrlString = s3Presigner.presignCreateMultipartUpload {
      it.createMultipartUploadRequest{
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    val uploadId = HttpPost(presignedUrlString)
      .let { post ->
        httpClient.execute(
        post
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        val result = MAPPER.readValue(it.entity.content, InitiateMultipartUploadResult::class.java)
        assertThat(result).isNotNull
        result
      }.uploadId
    }

    s3Client.listMultipartUploads {
      it.bucket(bucketName)
    }.also {
      assertThat(it.uploads()).hasSize(1)
      assertThat(it.uploads()[0].uploadId()).isEqualTo(uploadId)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_abortMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val createMultipartUpload = s3Client.createMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
    }

    val uploadId = createMultipartUpload.uploadId()
    s3Client.uploadPart(
      {
        it.bucket(createMultipartUpload.bucket())
        it.key(createMultipartUpload.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(UPLOAD_FILE_LENGTH)
      }, RequestBody.fromFile(UPLOAD_FILE),
    )

    val presignedUrlString = s3Presigner.presignAbortMultipartUpload {
      it.abortMultipartUploadRequest{
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpDelete(presignedUrlString).also { delete ->
      httpClient.execute(
        delete
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_NO_CONTENT)
      }
    }

    s3Client.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_completeMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val createMultipartUpload = s3Client.createMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
    }

    val uploadId = createMultipartUpload.uploadId()
    val uploadPartResult = s3Client.uploadPart(
      {
        it.bucket(createMultipartUpload.bucket())
        it.key(createMultipartUpload.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(UPLOAD_FILE_LENGTH)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    val presignedUrlString = s3Presigner.presignCompleteMultipartUpload {
      it.completeMultipartUploadRequest{
        it.bucket(bucketName)
        it.key(key)
        it.uploadId(uploadId)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpPost(presignedUrlString).apply {
      this.setHeader(BasicHeader("Content-Type", "application/xml"))
      this.entity = StringEntity(
        """<CompleteMultipartUpload>
        <Part>
          <ETag>${uploadPartResult.eTag()}</ETag>
          <PartNumber>1</PartNumber>
        </Part>
      </CompleteMultipartUpload>
      """)
    }.also { post ->
      httpClient.execute(
        post
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }

    s3Client.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }


  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_uploadPart(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val createMultipartUpload = s3Client.createMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
    }

    val uploadId = createMultipartUpload.uploadId()

    val presignedUrlString = s3Presigner.presignUploadPart {
      it.uploadPartRequest {
        it.bucket(createMultipartUpload.bucket())
        it.key(createMultipartUpload.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(UPLOAD_FILE_LENGTH)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpPut(presignedUrlString).apply {
      this.entity = FileEntity(UPLOAD_FILE)
    }.also { put ->
      httpClient.execute(
        put
      ).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        s3Client.completeMultipartUpload {
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
          it.multipartUpload {
            it.parts(
              CompletedPart
                .builder()
                .eTag(response.getFirstHeader("ETag").value)
                .partNumber(1)
                .build()
            )
          }
        }
      }
    }

    s3Client.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }
}
