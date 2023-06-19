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

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.ArrayUtils
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import java.io.File

internal class CrtAsyncV2IT : S3TestBase() {

  @Test
  fun testPutGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = randomName
    val bucketFuture =
      autoS3CrtAsyncClientV2.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
    bucketFuture.join()

    val putObjectFuture = autoS3CrtAsyncClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME).build(),
      AsyncRequestBody.fromFile(uploadFile)
    )
    val putObjectResponse = putObjectFuture.join()
    val eTag = putObjectResponse.eTag()

    val getObjectResponseCompletableFuture = autoS3CrtAsyncClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
      AsyncResponseTransformer.toBytes()
    )
    val getObjectResponse = getObjectResponseCompletableFuture.join()
    Assertions.assertThat(getObjectResponse.response().eTag()).isEqualTo(eTag)
  }

  @Test
  fun testMultipartUpload(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val objectMetadata = HashMap<String, String>()
    objectMetadata["key"] = "value"
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
    val uploadPartResponseCompletableFuture = autoS3CrtAsyncClientV2.uploadPart(
      UploadPartRequest
        .builder()
        .bucket(initiateMultipartUploadResult.bucket())
        .key(initiateMultipartUploadResult.key())
        .uploadId(uploadId)
        .partNumber(2)
        .contentLength(uploadFile.length()).build(),
      //.lastPart(true)
      AsyncRequestBody.fromFile(uploadFile),
    )

    val uploadPartResponse = uploadPartResponseCompletableFuture.join()

    val completeMultipartUploadResponseCompletableFuture =
      autoS3CrtAsyncClientV2.completeMultipartUpload(
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
      )
    val completeMultipartUploadResponse = completeMultipartUploadResponseCompletableFuture.join()

    // Verify only 1st and 3rd counts
    val getObjectResponseCompletableFuture = autoS3CrtAsyncClientV2.getObject(
      GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build(),
      AsyncResponseTransformer.toBytes()
    )
    val getObjectResponse = getObjectResponseCompletableFuture.join()

    val uploadFileBytes = readStreamIntoByteArray(uploadFile.inputStream())
    val allMd5s = ArrayUtils.addAll(
      DigestUtils.md5(randomBytes),
      *DigestUtils.md5(uploadFileBytes)
    )

    // verify special etag
    Assertions.assertThat(completeMultipartUploadResponse.eTag())
      .`as`("Special etag doesn't match.")
      .isEqualTo("\"" + DigestUtils.md5Hex(allMd5s) + "-2" + "\"")

    // verify content size
    Assertions.assertThat(getObjectResponse.response().contentLength())
      .`as`("Content length doesn't match")
      .isEqualTo(randomBytes.size.toLong() + uploadFileBytes.size.toLong())

    // verify contents
    Assertions.assertThat(readStreamIntoByteArray(getObjectResponse.asInputStream())).`as`(
      "Object contents doesn't match"
    ).isEqualTo(concatByteArrays(randomBytes, uploadFileBytes))

    Assertions.assertThat(completeMultipartUploadResponse.location())
      .matches("http.*/$bucketName/src%2Ftest%2Fresources%2FsampleFile.txt")
  }

  private fun uploadPart(
    bucketName: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    randomBytes: ByteArray
  ): String {
    val completableFuture = autoS3CrtAsyncClientV2
      .uploadPart(
        UploadPartRequest.builder()
          .bucket(bucketName)
          .key(key)
          .uploadId(uploadId)
          .partNumber(partNumber)
          .contentLength(randomBytes.size.toLong()).build(),
        AsyncRequestBody.fromBytes(randomBytes)
      )
    val uploadPartResponse = completableFuture.join()

    return uploadPartResponse.eTag()
  }
}
