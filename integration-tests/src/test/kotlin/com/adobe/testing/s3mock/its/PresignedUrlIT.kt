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

import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import tel.schich.awss3postobjectpresigner.S3PostObjectPresigner
import tel.schich.awss3postobjectpresigner.S3PostObjectRequest
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID

internal class PresignedUrlIT : S3TestBase() {
  private val httpClient: HttpClient = createHttpClient()
  private val s3Client: S3Client = createS3Client()
  private val s3Presigner: S3Presigner = createS3Presigner()
  private val s3PostObjectPresigner: S3PostObjectPresigner = createS3PostObjectPresigner()

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "S3PostObjectPresigner does not create working presigned URLs.",
  )
  fun testPresignedUrl_postObject_largeFile(testInfo: TestInfo) {
    val key = randomName
    val bucketName = givenBucket(testInfo)

    val presignedUrlString =
      s3PostObjectPresigner
        .presignPost(
          S3PostObjectRequest
            .builder()
            .bucket(bucketName)
            .expiration(Duration.ofMinutes(1L))
            .build(),
        ).uri()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val randomMBytes = randomMBytes(20)
    val expectedEtag = randomMBytes.inputStream().use { "\"${DigestUtil.hexDigest(it)}\"" }

    val boundary = UUID.randomUUID().toString().replace("-", "")
    val crlf = "\r\n"
    val body =
      buildString {
        fun textPart(
          name: String,
          value: String,
        ) {
          append("--$boundary$crlf")
          append("Content-Disposition: form-data; name=\"$name\"$crlf")
          append(crlf)
          append(value)
          append(crlf)
        }
        textPart("key", key)
        textPart("Content-Type", "application/octet-stream")
        textPart(X_AMZ_STORAGE_CLASS, "INTELLIGENT_TIERING")
        textPart(
          "tagging",
          "<Tagging><TagSet><Tag><Key>Tag Name</Key><Value>Tag Value</Value></Tag></TagSet></Tagging>",
        )
        append("--$boundary$crlf")
        append("Content-Disposition: form-data; name=\"file\"; filename=\"$key\"$crlf")
        append("Content-Type: application/octet-stream$crlf")
        append(crlf)
      }.toByteArray(Charsets.UTF_8) + randomMBytes + "$crlf--$boundary--$crlf".toByteArray(Charsets.UTF_8)

    val postRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .header("Content-Type", "multipart/form-data; boundary=$boundary")
        .build()

    val postResponse = httpClient.send(postRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(postResponse.statusCode()).isEqualTo(200)
    val actualEtag = postResponse.headers().firstValue("ETag").orElse(null)
    assertThat(actualEtag).isEqualTo(expectedEtag)

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        val actualEtag2 = "\"${DigestUtil.hexDigest(it)}\""
        assertThat(actualEtag2).isEqualTo(expectedEtag)
        assertThat(it.response().storageClass()).isEqualTo(StorageClass.INTELLIGENT_TIERING)
      }
    s3Client
      .getObjectTagging {
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

    val presignedUrlString =
      s3Presigner
        .presignGetObject {
          it.getObjectRequest {
            it.bucket(bucketName)
            it.key(key)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val expectedEtag =
      UPLOAD_FILE.inputStream().use {
        "\"${DigestUtil.hexDigest(it)}\""
      }
    val getRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .GET()
        .build()

    val response = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofByteArray())
    assertThat(response.statusCode()).isEqualTo(200)
    val actualEtag = "\"${DigestUtil.hexDigest(response.body().inputStream())}\""
    assertThat(actualEtag).isEqualTo(expectedEtag)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_getObject_responseHeaderOverrides(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)

    val responseExpires = Instant.now()

    val presignedUrlString =
      s3Presigner
        .presignGetObject {
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
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()
    val expectedEtag =
      UPLOAD_FILE.inputStream().use {
        "\"${DigestUtil.hexDigest(it)}\""
      }
    val getRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .GET()
        .build()

    val response = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofByteArray())
    assertThat(response.statusCode()).isEqualTo(200)
    val actualEtag = "\"${DigestUtil.hexDigest(response.body().inputStream())}\""
    assertThat(actualEtag).isEqualTo(expectedEtag)
    // TODO: S3 SDK serializes date as 'Sun, 20 Apr 2025 22:07:04 GMT'
    // assertThat(response.headers().firstValue("Expires").orElse(null)).isEqualTo(responseExpires)
    assertThat(response.headers().firstValue("Cache-Control").orElse(null)).isEqualTo("no-cache")
    assertThat(response.headers().firstValue("Content-Disposition").orElse(null))
      .isEqualTo("attachment; filename=\"$key\"")
    assertThat(response.headers().firstValue("Content-Encoding").orElse(null)).isEqualTo("encoding")
    assertThat(response.headers().firstValue("Content-Type").orElse(null)).isEqualTo("application/json")
    assertThat(response.headers().firstValue("Content-Language").orElse(null)).isEqualTo("en")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_getObject_range(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, key)

    val presignedUrlString =
      s3Presigner
        .presignGetObject {
          it.getObjectRequest {
            it.bucket(bucketName)
            it.key(key)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val getRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .GET()
        .header("Range", "bytes=0-100")
        .build()

    val response = httpClient.send(getRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(response.statusCode()).isEqualTo(206)
    assertThat(response.headers().firstValue("Content-Length").orElse(null)).isEqualTo("101")
    assertThat(response.headers().firstValue("Content-Range").orElse(null)).isEqualTo("bytes 0-100/63839")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPresignedUrl_putObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val presignedUrlString =
      s3Presigner
        .presignPutObject {
          it.putObjectRequest {
            it.bucket(bucketName)
            it.key(key)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val putRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .PUT(HttpRequest.BodyPublishers.ofFile(UPLOAD_FILE.toPath()))
        .build()
    val putResponse = httpClient.send(putRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(putResponse.statusCode()).isEqualTo(200)

    val expectedEtag =
      UPLOAD_FILE.inputStream().use {
        "\"${DigestUtil.hexDigest(it)}\""
      }
    s3Client
      .getObject {
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

    val presignedUrlString =
      s3Presigner
        .presignPutObject {
          it.putObjectRequest {
            it.bucket(bucketName)
            it.key(key)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val randomMBytes = randomMBytes(20)
    val putRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .PUT(HttpRequest.BodyPublishers.ofByteArray(randomMBytes))
        .build()
    val putResponse = httpClient.send(putRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(putResponse.statusCode()).isEqualTo(200)

    val expectedEtag = randomMBytes.inputStream().use { "\"${DigestUtil.hexDigest(it)}\"" }
    s3Client
      .getObject {
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

    val presignedUrlString =
      s3Presigner
        .presignCreateMultipartUpload {
          it.createMultipartUploadRequest {
            it.bucket(bucketName)
            it.key(key)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val postRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .POST(HttpRequest.BodyPublishers.noBody())
        .build()

    val uploadId =
      httpClient.send(postRequest, HttpResponse.BodyHandlers.ofByteArray()).let { response ->
        assertThat(response.statusCode()).isEqualTo(200)
        val result = MAPPER.readValue(response.body().inputStream(), InitiateMultipartUploadResult::class.java)
        assertThat(result).isNotNull
        result.uploadId
      }

    s3Client
      .listMultipartUploads {
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

    val createMultipartUpload =
      s3Client.createMultipartUpload {
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
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    val presignedUrlString =
      s3Presigner
        .presignAbortMultipartUpload {
          it.abortMultipartUploadRequest {
            it.bucket(bucketName)
            it.key(key)
            it.uploadId(uploadId)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val deleteRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .DELETE()
        .build()
    val deleteResponse = httpClient.send(deleteRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(deleteResponse.statusCode()).isEqualTo(204)

    s3Client
      .listMultipartUploads {
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

    val createMultipartUpload =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(key)
      }

    val uploadId = createMultipartUpload.uploadId()
    val uploadPartResult =
      s3Client.uploadPart(
        {
          it.bucket(createMultipartUpload.bucket())
          it.key(createMultipartUpload.key())
          it.uploadId(uploadId)
          it.partNumber(1)
          it.contentLength(UPLOAD_FILE_LENGTH)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )

    val presignedUrlString =
      s3Presigner
        .presignCompleteMultipartUpload {
          it.completeMultipartUploadRequest {
            it.bucket(bucketName)
            it.key(key)
            it.uploadId(uploadId)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val postRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .POST(
          HttpRequest.BodyPublishers.ofString(
            """<CompleteMultipartUpload>
        <Part>
          <ETag>${uploadPartResult.eTag()}</ETag>
          <PartNumber>1</PartNumber>
        </Part>
      </CompleteMultipartUpload>
      """,
          ),
        ).header("Content-Type", "application/xml")
        .build()
    val postResponse = httpClient.send(postRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(postResponse.statusCode()).isEqualTo(200)

    s3Client
      .listMultipartUploads {
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

    val createMultipartUpload =
      s3Client.createMultipartUpload {
        it.bucket(bucketName)
        it.key(key)
      }

    val uploadId = createMultipartUpload.uploadId()

    val presignedUrlString =
      s3Presigner
        .presignUploadPart {
          it.uploadPartRequest {
            it.bucket(createMultipartUpload.bucket())
            it.key(createMultipartUpload.key())
            it.uploadId(uploadId)
            it.partNumber(1)
            it.contentLength(UPLOAD_FILE_LENGTH)
          }
          it.signatureDuration(Duration.ofMinutes(1L))
        }.url()
        .toString()

    assertThat(presignedUrlString).isNotBlank()

    val putRequest =
      HttpRequest
        .newBuilder(URI.create(presignedUrlString))
        .PUT(HttpRequest.BodyPublishers.ofFile(UPLOAD_FILE.toPath()))
        .build()
    val putResponse = httpClient.send(putRequest, HttpResponse.BodyHandlers.discarding())
    assertThat(putResponse.statusCode()).isEqualTo(200)
    val eTag = putResponse.headers().firstValue("ETag").orElse(null)

    s3Client.completeMultipartUpload {
      it.bucket(bucketName)
      it.key(key)
      it.uploadId(uploadId)
      it.multipartUpload {
        it.parts(
          CompletedPart
            .builder()
            .eTag(eTag)
            .partNumber(1)
            .build(),
        )
      }
    }

    s3Client
      .listMultipartUploads {
        it.bucket(bucketName)
        it.keyMarker(key)
      }.also {
        assertThat(it.uploads()).isEmpty()
      }
  }
}
