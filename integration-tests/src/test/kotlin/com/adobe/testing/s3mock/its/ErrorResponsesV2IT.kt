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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import java.io.File

internal class ErrorResponsesV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun getObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val req = GetObjectRequest.builder().bucket(bucketName).key(NON_EXISTING_KEY).build()
    assertThatThrownBy { s3ClientV2.getObject(req) }.isInstanceOf(
      NoSuchKeyException::class.java
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedTodo
  fun getObject_noSuchKey_startingSlash(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val req = GetObjectRequest.builder().bucket(bucketName).key("/$NON_EXISTING_KEY").build()
    assertThatThrownBy { s3ClientV2.getObject(req) }.isInstanceOf(
      NoSuchKeyException::class.java
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedTodo
  fun putObject_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy {
      s3ClientV2.putObject(
        PutObjectRequest
          .builder()
          .bucket(randomName)
          .key(UPLOAD_FILE_NAME)
          .build(),
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun copyObjectToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy { s3ClientV2.copyObject(
      software.amazon.awssdk.services.s3.model.CopyObjectRequest
        .builder()
        .sourceBucket(bucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destinationBucketName)
        .destinationKey(destinationKey)
        .build()
    ) }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun deleteObject_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteObject(
        DeleteObjectRequest
          .builder()
          .bucket(randomName)
          .key(NON_EXISTING_KEY)
          .build()
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun deleteObject_nonExistent_OK(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    s3ClientV2.deleteObject(
      DeleteObjectRequest
        .builder()
        .bucket(bucketName)
        .key(NON_EXISTING_KEY)
        .build()
    )
  }

  @Test
  @S3VerifiedTodo
  fun deleteObjects_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteObjects(
        DeleteObjectsRequest
          .builder()
          .bucket(randomName)
          .delete(
            Delete
              .builder()
              .objects(ObjectIdentifier
                .builder()
                .key(NON_EXISTING_KEY)
                .build()
              ).build()
          ).build()
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun deleteBucket_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.deleteBucket(
        DeleteBucketRequest
          .builder()
          .bucket(randomName)
          .build()
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun multipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.createMultipartUpload(
        CreateMultipartUploadRequest
          .builder()
          .bucket(randomName)
          .key(UPLOAD_FILE_NAME)
          .build()
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun listMultipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest
          .builder()
          .bucket(randomName)
          .build()
      )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun abortMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3ClientV2.abortMultipartUpload(
        AbortMultipartUploadRequest
          .builder()
          .bucket(randomName)
          .key(UPLOAD_FILE_NAME)
          .uploadId("uploadId")
          .build()
        )
    }
      .isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedTodo
  fun uploadMultipart_invalidPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3ClientV2
      .createMultipartUpload(
        CreateMultipartUploadRequest
          .builder()
          .bucket(bucketName)
          .key(UPLOAD_FILE_NAME)
          .build()
      )
    val uploadId = initiateMultipartUploadResult.uploadId()
    assertThat(
      s3ClientV2.listMultipartUploads(
        ListMultipartUploadsRequest
          .builder()
          .bucket(bucketName)
          .build()
      ).uploads()
    ).isNotEmpty
    val invalidPartNumber = 0
    assertThatThrownBy {
      s3ClientV2.uploadPart(
        UploadPartRequest
          .builder()
          .bucket(initiateMultipartUploadResult.bucket())
          .key(initiateMultipartUploadResult.key())
          .uploadId(uploadId)
          .partNumber(invalidPartNumber)
          .build(),
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining(INVALID_PART_NUMBER)
  }

  companion object {
    private const val NON_EXISTING_KEY = "NoSuchKey.json"
    private const val NO_SUCH_KEY = "The specified key does not exist."
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
    private const val STATUS_CODE_404 = "Status Code: 404"
    private const val INVALID_PART_NUMBER = "Part number must be an integer between 1 and 10000, inclusive"
    private const val INVALID_PART = "Status Code: 400; Error Code: InvalidPart"
  }
}
