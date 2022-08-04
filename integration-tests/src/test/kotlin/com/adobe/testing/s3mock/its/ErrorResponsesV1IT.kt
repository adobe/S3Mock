/*
 *  Copyright 2017-2022 Adobe.
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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.File
import java.util.UUID

/**
 * Test the application using the AmazonS3 SDK V1.
 * Verifies S3 Mocks Error Responses.
 */
class ErrorResponsesV1IT : S3TestBase() {
  /**
   * Verifies that `NoSuchBucket` is returned in Error Response if `putObject`
   * references a non-existing Bucket.
   */
  @Test
  fun putObjectOnNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    assertThatThrownBy {
      s3Client!!.putObject(
        PutObjectRequest(
          BUCKET_NAME,
          UPLOAD_FILE_NAME,
          uploadFile
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Verifies that `NoSuchBucket` is returned in Error Response if `putObject`
   * references a non-existing Bucket.
   */
  @Test
  fun putObjectEncryptedOnNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val putObjectRequest = PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
    putObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    assertThatThrownBy {
      s3Client!!.putObject(
        PutObjectRequest(
          BUCKET_NAME,
          UPLOAD_FILE_NAME,
          uploadFile
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Verifies that `NoSuchBucket` is returned in Error Response if `copyObject`
   * references a non-existing destination Bucket.
   */
  @Test
  fun copyObjectToNonExistingDestinationBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Verifies that `NoSuchBucket` is returned in Error Response if `copyObject`
   * encrypted references a non-existing destination Bucket.
   */
  @Test
  fun copyObjectEncryptedToNonExistingDestinationBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = "destinationbucket"
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(BUCKET_NAME, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams =
      SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if the Metadata of an existing file can be retrieved.
   */
  @Test
  fun objectMetadataWithNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val objectMetadata = ObjectMetadata()
    objectMetadata.addUserMetadata("key", "value")
    s3Client!!.putObject(
      PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile)
        .withMetadata(objectMetadata)
    )
    assertThatThrownBy {
      s3Client!!.getObjectMetadata(
        UUID.randomUUID().toString(),
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  /**
   * Verifies that `NO_SUCH_KEY` is returned in Error Response if `getObject`
   * on a non-existing Object.
   */
  @Test
  fun nonExistingObject() {
    s3Client!!.createBucket(BUCKET_NAME)
    val getObjectRequest = GetObjectRequest(BUCKET_NAME, "NoSuchKey.json")
    assertThatThrownBy { s3Client!!.getObject(getObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_KEY)
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  fun deleteFromNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    assertThatThrownBy {
      s3Client!!.deleteObject(
        UUID.randomUUID().toString(),
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if deleting an Object returns `204 No Content` even of the given key does not
   * exist.
   */
  @Test
  fun deleteNonExistingObject() {
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.deleteObject(BUCKET_NAME, UUID.randomUUID().toString())
  }

  /**
   * Tests if an object can be deleted.
   */
  @Test
  fun batchDeleteObjectsFromNonExistingBucket() {
    val uploadFile1 = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.putObject(PutObjectRequest(BUCKET_NAME, "1_$UPLOAD_FILE_NAME", uploadFile1))
    val multiObjectDeleteRequest = DeleteObjectsRequest(UUID.randomUUID().toString())
    val keys: MutableList<KeyVersion> = ArrayList()
    keys.add(KeyVersion("1_$UPLOAD_FILE_NAME"))
    multiObjectDeleteRequest.keys = keys
    assertThatThrownBy { s3Client!!.deleteObjects(multiObjectDeleteRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests that a bucket can be deleted.
   */
  @Test
  fun deleteNonExistingBucket() {
    assertThatThrownBy { s3Client!!.deleteBucket(BUCKET_NAME) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if the list objects can be retrieved.
   */
  @Test
  fun listObjectsFromNonExistingBucket() {
    assertThatThrownBy {
      s3Client!!.listObjects(
        UUID.randomUUID().toString(),
        UPLOAD_FILE_NAME
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if an object can be uploaded asynchronously.
   *
   */
  @Test
  fun uploadParallelToNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val transferManager = createTransferManager()
    assertThatThrownBy {
      val upload = transferManager.upload(
        PutObjectRequest(UUID.randomUUID().toString(), UPLOAD_FILE_NAME, uploadFile)
      )
      upload.waitForUploadResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  fun multipartUploadsToNonExistingBucket() {
    assertThatThrownBy {
      s3Client!!.initiateMultipartUpload(
        InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  fun listMultipartUploadsFromNonExistingBucket() {
    s3Client!!.createBucket(BUCKET_NAME)
    s3Client!!.initiateMultipartUpload(
      InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME)
    )
    assertThatThrownBy {
      s3Client!!.listMultipartUploads(
        ListMultipartUploadsRequest(UUID.randomUUID().toString())
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  fun abortMultipartUploadInNonExistingBucket() {
    s3Client!!.createBucket(BUCKET_NAME)
    val initiateMultipartUploadResult = s3Client!!
      .initiateMultipartUpload(InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME))
    val uploadId = initiateMultipartUploadResult.uploadId
    assertThat(
      s3Client!!.listMultipartUploads(ListMultipartUploadsRequest(BUCKET_NAME))
        .multipartUploads
    ).isNotEmpty
    assertThatThrownBy {
      s3Client!!.abortMultipartUpload(
        AbortMultipartUploadRequest(
          UUID.randomUUID().toString(),
          UPLOAD_FILE_NAME,
          uploadId
        )
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  fun uploadMultipartWithInvalidPartNumber() {
    s3Client!!.createBucket(BUCKET_NAME)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client!!
      .initiateMultipartUpload(InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME))
    val uploadId = initiateMultipartUploadResult.uploadId
    assertThat(
      s3Client!!.listMultipartUploads(ListMultipartUploadsRequest(BUCKET_NAME))
        .multipartUploads
    ).isNotEmpty
    val invalidPartNumber = 0
    assertThatThrownBy {
      s3Client!!.uploadPart(
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
      .hasMessageContaining(INVALID_REQUEST)
  }

  @Test
  fun completeMultipartUploadWithNonExistingPartNumber() {
    s3Client!!.createBucket(BUCKET_NAME)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val initiateMultipartUploadResult = s3Client!!
      .initiateMultipartUpload(InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME))
    val uploadId = initiateMultipartUploadResult.uploadId
    assertThat(
      s3Client!!.listMultipartUploads(ListMultipartUploadsRequest(BUCKET_NAME))
        .multipartUploads
    ).isNotEmpty
    val partETag = s3Client!!.uploadPart(
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
      s3Client!!.completeMultipartUpload(
        CompleteMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId, partETags)
      )
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(INVALID_PART)
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  @Throws(Exception::class)
  fun rangeDownloadsFromNonExistingBucket() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val transferManager = createTransferManager()
    val upload =
      transferManager.upload(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    upload.waitForUploadResult()
    val downloadFile = File.createTempFile(UUID.randomUUID().toString(), null)
    assertThatThrownBy {
      transferManager.download(
        GetObjectRequest(UUID.randomUUID().toString(), UPLOAD_FILE_NAME).withRange(1, 2),
        downloadFile
      ).waitForCompletion()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  /**
   * Verify that range-downloads work.
   *
   * @throws Exception not expected
   */
  @Test
  @Throws(Exception::class)
  fun rangeDownloadsFromNonExistingObject() {
    val uploadFile = File(UPLOAD_FILE_NAME)
    s3Client!!.createBucket(BUCKET_NAME)
    val transferManager = createTransferManager()
    val upload =
      transferManager.upload(PutObjectRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadFile))
    upload.waitForUploadResult()
    val downloadFile = File.createTempFile(UUID.randomUUID().toString(), null)
    assertThatThrownBy {
      transferManager.download(
        GetObjectRequest(BUCKET_NAME, UUID.randomUUID().toString()).withRange(1, 2),
        downloadFile
      ).waitForCompletion()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  @Throws(InterruptedException::class)
  fun multipartCopyToNonExistingBucket() {
    //content larger than default part threshold of 5MiB
    val contentLen = 7 * _1MB
    val objectMetadata = ObjectMetadata()
    objectMetadata.contentLength = contentLen.toLong()
    val assumedSourceKey = UUID.randomUUID().toString()
    val sourceBucket = s3Client!!.createBucket(UUID.randomUUID().toString())
    val transferManager = createTransferManager()
    val sourceInputStream = randomInputStream(contentLen)
    val upload = transferManager
      .upload(
        sourceBucket.name, assumedSourceKey,
        sourceInputStream, objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = UUID.randomUUID().toString()
    assertThatThrownBy {
      transferManager.copy(
        sourceBucket.name, assumedSourceKey,
        UUID.randomUUID().toString(), assumedDestinationKey
      ).waitForCopyResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  /**
   * Verifies multipart copy.
   */
  @Test
  @Throws(InterruptedException::class)
  fun multipartCopyNonExistingObject() {
    //content larger than default part threshold of 5MiB
    val contentLen = 7 * _1MB
    val objectMetadata = ObjectMetadata()
    objectMetadata.contentLength = contentLen.toLong()
    val assumedSourceKey = UUID.randomUUID().toString()
    val sourceBucket = s3Client!!.createBucket(UUID.randomUUID().toString())
    val targetBucket = s3Client!!.createBucket(UUID.randomUUID().toString())
    val transferManager = createTransferManager()
    val sourceInputStream = randomInputStream(contentLen)
    val upload = transferManager
      .upload(
        sourceBucket.name, assumedSourceKey,
        sourceInputStream, objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = UUID.randomUUID().toString()
    assertThatThrownBy {
      transferManager.copy(
        sourceBucket.name, UUID.randomUUID().toString(),
        targetBucket.name, assumedDestinationKey
      ).waitForCopyResult()
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining(STATUS_CODE_404)
  }

  companion object {
    private const val NO_SUCH_BUCKET = "Status Code: 404; Error Code: NoSuchBucket"
    private const val NO_SUCH_KEY = "Status Code: 404; Error Code: NoSuchKey"
    private const val STATUS_CODE_404 = "Status Code: 404"
    private const val INVALID_REQUEST = "Status Code: 400; Error Code: InvalidRequest"
    private const val INVALID_PART = "Status Code: 400; Error Code: InvalidPart"
  }
}
