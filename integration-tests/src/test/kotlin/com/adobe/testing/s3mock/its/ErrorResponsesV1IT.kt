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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import com.amazonaws.services.s3.model.UploadPartRequest
import com.amazonaws.services.s3.transfer.TransferManager
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.File
import java.util.UUID

/**
 * Test the application using the AmazonS3 SDK V1.
 * Verifies S3 Mocks Error Responses.
 */
internal class ErrorResponsesV1IT : S3TestBase() {

  private val s3Client: AmazonS3 = createS3ClientV1()
  private val transferManagerV1: TransferManager = createTransferManagerV1()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val getObjectRequest = GetObjectRequest(bucketName, NON_EXISTING_KEY)
    assertThatThrownBy { s3Client.getObject(getObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObject_noSuchKey_startingSlash(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val getObjectRequest = GetObjectRequest(bucketName, "/$NON_EXISTING_KEY")
    assertThatThrownBy { s3Client.getObject(getObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun putObject_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy {
      s3Client.putObject(
        PutObjectRequest(
          randomName,
          UPLOAD_FILE_NAME,
          uploadFile
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun putObjectEncrypted_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    PutObjectRequest(randomName, UPLOAD_FILE_NAME, uploadFile).apply {
      this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    }
    assertThatThrownBy {
      s3Client.putObject(
        PutObjectRequest(
          randomName,
          UPLOAD_FILE_NAME,
          uploadFile
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun copyObjectToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, UPLOAD_FILE_NAME)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    assertThatThrownBy { s3Client.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun copyObjectEncryptedToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).apply {
      this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    }
    assertThatThrownBy { s3Client.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun getObjectMetadata_noSuchBucket() {
    assertThatThrownBy {
      s3Client.getObjectMetadata(
        randomName,
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteFrom_noSuchBucket() {
    assertThatThrownBy {
      s3Client.deleteObject(
        randomName,
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteObject_nonExistent_OK(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    s3Client.deleteObject(bucketName, randomName)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun batchDeleteObjects_noSuchBucket() {
    val multiObjectDeleteRequest = DeleteObjectsRequest(randomName).apply {
      this.keys = listOf(KeyVersion("1_$UPLOAD_FILE_NAME"))
    }
    assertThatThrownBy { s3Client.deleteObjects(multiObjectDeleteRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun deleteBucket_noSuchBucket() {
    assertThatThrownBy { s3Client.deleteBucket(randomName) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun listObjects_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listObjects(
        randomName,
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun uploadParallel_noSuchBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy {
      val upload = transferManagerV1.upload(
        PutObjectRequest(randomName, UPLOAD_FILE_NAME, uploadFile)
      )
      upload.waitForUploadResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun multipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3Client.initiateMultipartUpload(
        InitiateMultipartUploadRequest(randomName, UPLOAD_FILE_NAME)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun listMultipartUploads_noSuchBucket() {
    assertThatThrownBy {
      s3Client.listMultipartUploads(
        ListMultipartUploadsRequest(randomName)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun abortMultipartUpload_noSuchBucket() {
    assertThatThrownBy {
      s3Client.abortMultipartUpload(
        AbortMultipartUploadRequest(
          randomName,
          UPLOAD_FILE_NAME,
          "uploadId"
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun uploadMultipart_invalidPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .initiateMultipartUpload(InitiateMultipartUploadRequest(bucketName, UPLOAD_FILE_NAME))
    val uploadId = initiateMultipartUploadResult.uploadId
    assertThat(
      s3Client.listMultipartUploads(ListMultipartUploadsRequest(bucketName))
        .multipartUploads
    ).isNotEmpty
    val invalidPartNumber = 0
    assertThatThrownBy {
      s3Client.uploadPart(
        UploadPartRequest()
          .withBucketName(initiateMultipartUploadResult.bucketName)
          .withKey(initiateMultipartUploadResult.key)
          .withUploadId(uploadId)
          .withFile(uploadFile)
          .withFileOffset(0)
          .withPartNumber(invalidPartNumber)
          .withPartSize(uploadFile.length())
          .withLastPart(true)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(INVALID_PART_NUMBER)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun completeMultipartUploadWithNonExistingPartNumber(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client
      .initiateMultipartUpload(InitiateMultipartUploadRequest(bucketName, UPLOAD_FILE_NAME))
    val uploadId = initiateMultipartUploadResult.uploadId
    assertThat(
      s3Client.listMultipartUploads(ListMultipartUploadsRequest(bucketName))
        .multipartUploads
    ).isNotEmpty
    val partETag = s3Client.uploadPart(
      UploadPartRequest()
        .withBucketName(initiateMultipartUploadResult.bucketName)
        .withKey(initiateMultipartUploadResult.key)
        .withUploadId(uploadId)
        .withFile(uploadFile)
        .withFileOffset(0)
        .withPartNumber(1)
        .withPartSize(uploadFile.length())
        .withLastPart(true)
    ).partETag

    // Set to non-existing part number
    partETag.partNumber = 2
    val partETags = listOf(partETag)
    assertThatThrownBy {
      s3Client.completeMultipartUpload(
        CompleteMultipartUploadRequest(bucketName, UPLOAD_FILE_NAME, uploadId, partETags)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(INVALID_PART)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  @Throws(Exception::class)
  fun rangeDownloadsFromNonExistingBucket() {
    val transferManager = createTransferManagerV1()
    val downloadFile = File.createTempFile(UUID.randomUUID().toString(), null)
    assertThatThrownBy {
      transferManager.download(
        GetObjectRequest(randomName, UPLOAD_FILE_NAME).withRange(1, 2),
        downloadFile
      ).waitForCompletion()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  @Throws(Exception::class)
  fun rangeDownloadsFromNonExistingObject(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val upload = transferManagerV1.upload(PutObjectRequest(bucketName, UPLOAD_FILE_NAME, uploadFile))
    upload.waitForUploadResult()
    val downloadFile = File.createTempFile(UUID.randomUUID().toString(), null)
    assertThatThrownBy {
      transferManagerV1.download(
        GetObjectRequest(bucketName, randomName).withRange(1, 2),
        downloadFile
      ).waitForCompletion()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  @Throws(InterruptedException::class)
  fun multipartCopyToNonExistingBucket(testInfo: TestInfo) {
    val sourceBucket = givenBucketV1(testInfo)
    val destinationBucket = randomName
    //content larger than default part threshold of 5MiB
    val contentLen = 7 * _1MB
    val objectMetadata = ObjectMetadata().apply {
      this.contentLength = contentLen.toLong()
    }
    val assumedSourceKey = randomName
    val sourceInputStream = randomInputStream(contentLen)
    val upload = transferManagerV1
      .upload(
        sourceBucket, assumedSourceKey,
        sourceInputStream, objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = randomName
    assertThatThrownBy {
      transferManagerV1.copy(
        sourceBucket,
        assumedSourceKey,
        destinationBucket,
        assumedDestinationKey
      ).waitForCopyResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  @Throws(InterruptedException::class)
  fun multipartCopyNonExistingObject(testInfo: TestInfo) {
    val sourceBucket = givenBucketV1(testInfo)
    val targetBucket = givenRandomBucketV1()
    //content larger than default part threshold of 5MiB
    val contentLen = 7 * _1MB
    val objectMetadata = ObjectMetadata().apply {
      this.contentLength = contentLen.toLong()
    }
    val assumedSourceKey = randomName
    val sourceInputStream = randomInputStream(contentLen)
    val upload = transferManagerV1
      .upload(
        sourceBucket, assumedSourceKey,
        sourceInputStream, objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = randomName
    assertThatThrownBy {
      transferManagerV1.copy(
        sourceBucket, randomName,
        targetBucket, assumedDestinationKey
      ).waitForCopyResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  companion object {
    private const val NON_EXISTING_KEY = "NoSuchKey.json"
    private const val NO_SUCH_BUCKET = "Status Code: 404; Error Code: NoSuchBucket"
    private const val NO_SUCH_KEY = "Status Code: 404; Error Code: NoSuchKey"
    private const val STATUS_CODE_404 = "Status Code: 404"
    private const val INVALID_PART_NUMBER = "Status Code: 400; Error Code: InvalidArgument"
    private const val INVALID_PART = "Status Code: 400; Error Code: InvalidPart"
  }
}
