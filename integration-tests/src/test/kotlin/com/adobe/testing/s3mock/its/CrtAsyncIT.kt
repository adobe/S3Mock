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

import com.adobe.testing.s3mock.util.DigestUtil
import org.apache.commons.codec.digest.DigestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.springframework.web.util.UriUtils
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets

internal class CrtAsyncIT : S3TestBase() {

  private val autoS3CrtAsyncClient: S3AsyncClient = createAutoS3CrtAsyncClient()
  private val transferManager: S3TransferManager = createTransferManager()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPutObject_etagCreation(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val bucketName = randomName
    autoS3CrtAsyncClient
      .createBucket {
        it.bucket(bucketName)
      }.join()

    val putObjectResponse = autoS3CrtAsyncClient.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      AsyncRequestBody.fromFile(uploadFile)
    ).join()

    putObjectResponse.eTag().also {
      assertThat(it).isNotBlank
      assertThat(it).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPutGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = randomName
    autoS3CrtAsyncClient
      .createBucket {
        it.bucket(bucketName)
      }.join()

    val eTag = autoS3CrtAsyncClient.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      AsyncRequestBody.fromFile(uploadFile)
    ).join().eTag()

    autoS3CrtAsyncClient.getObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      AsyncResponseTransformer.toBytes()
    ).join().also {
      assertThat(it.response().eTag()).isEqualTo(eTag)
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val createMultipartUploadResponseCompletableFuture = autoS3CrtAsyncClient
      .createMultipartUpload {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(objectMetadata)
      }
    val initiateMultipartUploadResult = createMultipartUploadResponseCompletableFuture.join()
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, >5MB
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    // upload part 2, <5MB
    val uploadPartResponse = autoS3CrtAsyncClient.uploadPart(
      {
        it.bucket(initiateMultipartUploadResult.bucket())
        it.key(initiateMultipartUploadResult.key())
        it.uploadId(uploadId)
        it.partNumber(2)
        it.contentLength(uploadFile.length())
        //it.lastPart(true)
      },
      AsyncRequestBody.fromFile(uploadFile),
    ).join()

    val completeMultipartUploadResponse = autoS3CrtAsyncClient.completeMultipartUpload {
      it.bucket(initiateMultipartUploadResult.bucket())
      it.key(initiateMultipartUploadResult.key())
      it.uploadId(initiateMultipartUploadResult.uploadId())
      it.multipartUpload {
        it.parts(
          {
            it.eTag(partETag)
            it.partNumber(1)
          },
          {
            it.eTag(uploadPartResponse.eTag())
            it.partNumber(2)
          })
      }
    }.join()

    // Verify only 1st and 3rd counts
    val getObjectResponse = autoS3CrtAsyncClient.getObject({
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }, AsyncResponseTransformer.toBytes()).join()

    val uploadFileBytes = readStreamIntoByteArray(uploadFile.inputStream())
    (DigestUtils.md5(randomBytes) + DigestUtils.md5(uploadFileBytes)).also {
      // verify special etag
      assertThat(completeMultipartUploadResponse.eTag()).isEqualTo("\"${DigestUtils.md5Hex(it)}-2\"")
    }

    // verify content size
    assertThat(getObjectResponse.response().contentLength())
      .isEqualTo(randomBytes.size.toLong() + uploadFileBytes.size.toLong())

    // verify contents
    assertThat(readStreamIntoByteArray(getObjectResponse.asInputStream()))
      .isEqualTo(concatByteArrays(randomBytes, uploadFileBytes))

    assertThat(completeMultipartUploadResponse.location())
      .matches("http.*/$bucketName/${UriUtils.encode(UPLOAD_FILE_NAME, StandardCharsets.UTF_8)}")
  }

  private fun uploadPart(
    bucketName: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    randomBytes: ByteArray
  ): String {
    return autoS3CrtAsyncClient
      .uploadPart(
        {
          it.bucket(bucketName)
          it.key(key)
          it.uploadId(uploadId)
          it.partNumber(partNumber)
          it.contentLength(randomBytes.size.toLong())
        },
        AsyncRequestBody.fromBytes(randomBytes)
      ).join()
      .eTag()
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testStreamUploadOfUnknownSize(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val body = AsyncRequestBody.forBlockingInputStream(null)
    val putObjectResponseFuture = autoS3CrtAsyncClient.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      body
    )

    val randomBytes = randomBytes()
    body.writeInputStream(ByteArrayInputStream(randomBytes))

    putObjectResponseFuture.join()

    val getObjectResponse = autoS3CrtAsyncClient.getObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      AsyncResponseTransformer.toBytes()
    ).join()

    // verify content size
    assertThat(getObjectResponse.response().contentLength())
      .isEqualTo(randomBytes.size.toLong())

    // verify contents
    assertThat(getObjectResponse.asByteArrayUnsafe())
      .isEqualTo(randomBytes)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testStreamUploadOfUnknownSize_transferManager(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val body = AsyncRequestBody.forBlockingInputStream(null)
    val upload = transferManager
      .upload {
        it.requestBody(body)
        it.putObjectRequest {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }
      }

    val randomBytes = randomBytes()
    body.writeInputStream(ByteArrayInputStream(randomBytes))

    upload.completionFuture().join()

    val download = transferManager
      .download(
        DownloadRequest
          .builder()
          .getObjectRequest {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
          }
          .responseTransformer(AsyncResponseTransformer.toBytes())
          .build()
      ).completionFuture().join().result()

    // verify content size
    assertThat(download.response().contentLength())
      .isEqualTo(randomBytes.size.toLong())

    // verify contents
    assertThat(download.asByteArrayUnsafe())
      .isEqualTo(randomBytes)
  }
}
