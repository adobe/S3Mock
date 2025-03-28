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
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.FileEntity
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

internal class PresignedUrlV2IT : S3TestBase() {
  private val httpClient: CloseableHttpClient = createHttpClient()
  private val s3ClientV2: S3Client = createS3ClientV2()
  private val s3Presigner: S3Presigner = createS3Presigner()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPresignedUrl_getObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, key)

    val presignedUrlString = s3Presigner.presignGetObject {
      it.getObjectRequest {
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpGet(presignedUrlString).also { get ->
      httpClient.execute(
        get
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        val expectedEtag = "\"${DigestUtil.hexDigest(Files.newInputStream(Path.of(UPLOAD_FILE_NAME)))}\""
        val actualEtag = "\"${DigestUtil.hexDigest(it.entity.content)}\""
        assertThat(actualEtag).isEqualTo(expectedEtag)
      }
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPresignedUrl_getObject_range(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, key)

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
  @S3VerifiedSuccess(year = 2024)
  fun testPresignedUrl_putObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucketV2(testInfo)

    val presignedUrlString = s3Presigner.presignGetObject {
      it.getObjectRequest{
        it.bucket(bucketName)
        it.key(key)
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpPut(presignedUrlString).apply {
      this.entity = FileEntity(File(UPLOAD_FILE_NAME))
    }.also { put ->
      httpClient.execute(
        put
      ).use {
        assertThat(it.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
      }
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      val expectedEtag = "\"${DigestUtil.hexDigest(Files.newInputStream(Path.of(UPLOAD_FILE_NAME)))}\""
      val actualEtag = "\"${DigestUtil.hexDigest(it)}\""
      assertThat(actualEtag).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedFailure(year = 2024, reason = "S3 returns no multipart uploads.")
  fun testPresignedUrl_createMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucketV2(testInfo)

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

    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
      it.uploadIdMarker(uploadId)
    }.also {
      assertThat(it.uploads()).hasSize(1)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPresignedUrl_abortMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucketV2(testInfo)
    val file = File(UPLOAD_FILE_NAME)

    val createMultipartUpload = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
    }

    val uploadId = createMultipartUpload.uploadId()
    s3ClientV2.uploadPart(
      {
        it.bucket(createMultipartUpload.bucket())
        it.key(createMultipartUpload.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(file.length())
      }, RequestBody.fromFile(file),
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

    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPresignedUrl_completeMultipartUpload(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucketV2(testInfo)
    val file = File(UPLOAD_FILE_NAME)

    val createMultipartUpload = s3ClientV2.createMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
    }

    val uploadId = createMultipartUpload.uploadId()
    val uploadPartResult = s3ClientV2.uploadPart(
      {
        it.bucket(createMultipartUpload.bucket())
        it.key(createMultipartUpload.key())
        it.uploadId(uploadId)
        it.partNumber(1)
        it.contentLength(file.length())
      },
      RequestBody.fromFile(file),
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

    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }


  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPresignedUrl_uploadPart(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucketV2(testInfo)
    val file = File(UPLOAD_FILE_NAME)

    val createMultipartUpload = s3ClientV2.createMultipartUpload {
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
        it.contentLength(file.length())
      }
      it.signatureDuration(Duration.ofMinutes(1L))
    }.url().toString()

    assertThat(presignedUrlString).isNotBlank()

    HttpPut(presignedUrlString).apply {
      this.entity = FileEntity(File(UPLOAD_FILE_NAME))
    }.also { put ->
      httpClient.execute(
        put
      ).use { response ->
        assertThat(response.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
        s3ClientV2.completeMultipartUpload {
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

    s3ClientV2.listMultipartUploads {
      it.bucket(bucketName)
      it.keyMarker(key)
    }.also {
      assertThat(it.uploads()).isEmpty()
    }
  }
}
