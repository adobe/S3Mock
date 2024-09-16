/*
 *  Copyright 2017-2024 Adobe.
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
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import software.amazon.awssdk.transfer.s3.model.UploadRequest
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets

internal class CrtAsyncV2IT : S3TestBase() {

  private val autoS3CrtAsyncClientV2: S3AsyncClient = createAutoS3CrtAsyncClientV2()
  private val transferManagerV2: S3TransferManager = createTransferManagerV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testPutObject_etagCreation(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val bucketName = randomName
    autoS3CrtAsyncClientV2
      .createBucket(CreateBucketRequest
        .builder()
        .bucket(bucketName)
        .build()
      ).join()

    val putObjectResponse = autoS3CrtAsyncClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
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
    autoS3CrtAsyncClientV2
      .createBucket(CreateBucketRequest
        .builder()
        .bucket(bucketName)
        .build()
      ).join()

    val eTag = autoS3CrtAsyncClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME).build(),
      AsyncRequestBody.fromFile(uploadFile)
    ).join().eTag()

    autoS3CrtAsyncClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
      AsyncResponseTransformer.toBytes()
    ).join().also {
      assertThat(it.response().eTag()).isEqualTo(eTag)
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = mapOf(Pair("key", "value"))
    val createMultipartUploadResponseCompletableFuture = autoS3CrtAsyncClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(UPLOAD_FILE_NAME)
          .metadata(objectMetadata).build()
      )
    val initiateMultipartUploadResult = createMultipartUploadResponseCompletableFuture.join()
    val uploadId = initiateMultipartUploadResult.uploadId()
    // upload part 1, >5MB
    val randomBytes = randomBytes()
    val partETag = uploadPart(bucketName, UPLOAD_FILE_NAME, uploadId, 1, randomBytes)
    // upload part 2, <5MB
    val uploadPartResponse = autoS3CrtAsyncClientV2.uploadPart(
      UploadPartRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(uploadId)
        .partNumber(2)
        .contentLength(uploadFile.length()).build(),
      //.lastPart(true)
      AsyncRequestBody.fromFile(uploadFile),
    ).join()

    val completeMultipartUploadResponse = autoS3CrtAsyncClientV2.completeMultipartUpload(
      CompleteMultipartUploadRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(initiateMultipartUploadResult.uploadId())
        .multipartUpload(
          CompletedMultipartUpload
            .builder()
            .parts(
              CompletedPart
                .builder()
                .eTag(partETag)
                .partNumber(1)
                .build(),
              CompletedPart
                .builder()
                .eTag(uploadPartResponse.eTag())
                .partNumber(2)
                .build()
            )
            .build()
        )
        .build()
    ).join()

    // Verify only 1st and 3rd counts
    val getObjectResponse = autoS3CrtAsyncClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
      AsyncResponseTransformer.toBytes()
    ).join()

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
    return autoS3CrtAsyncClientV2
      .uploadPart(
        UploadPartRequest.builder()
          .bucket(bucketName)
          .key(key)
          .uploadId(uploadId)
          .partNumber(partNumber)
          .contentLength(randomBytes.size.toLong()).build(),
        AsyncRequestBody.fromBytes(randomBytes)
      ).join()
      .eTag()
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testStreamUploadOfUnknownSize(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)

    val body = AsyncRequestBody.forBlockingInputStream(null)
    val putObjectResponseFuture = autoS3CrtAsyncClientV2.putObject(
      PutObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
      body
    )

    val randomBytes = randomBytes()
    body.writeInputStream(ByteArrayInputStream(randomBytes))

    putObjectResponseFuture.join()

    val getObjectResponse = autoS3CrtAsyncClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
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
    val bucketName = givenBucketV2(testInfo)

    val body = AsyncRequestBody.forBlockingInputStream(null)
    val upload = transferManagerV2
      .upload(
        UploadRequest
          .builder()
          .requestBody(body)
          .putObjectRequest(
            PutObjectRequest
              .builder()
              .bucket(bucketName)
              .key(UPLOAD_FILE_NAME)
              .build()
          )
          .build()
      )

    val randomBytes = randomBytes()
    body.writeInputStream(ByteArrayInputStream(randomBytes))

    upload.completionFuture().join()

    val download = transferManagerV2
      .download(
        DownloadRequest
          .builder()
          .getObjectRequest(
            GetObjectRequest
              .builder()
              .bucket(bucketName)
              .key(UPLOAD_FILE_NAME)
              .build()
          )
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
