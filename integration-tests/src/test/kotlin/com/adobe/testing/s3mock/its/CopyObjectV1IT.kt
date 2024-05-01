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
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.MetadataDirective
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import com.amazonaws.services.s3.transfer.TransferManager
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.UUID

/**
 * Test the application using the AmazonS3 SDK V1.
 */
internal class CopyObjectV1IT : S3TestBase() {

  private val s3Client: AmazonS3 = createS3ClientV1()
  private val transferManagerV1: TransferManager = createTransferManagerV1()

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObject(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).also {
      s3Client.copyObject(it)
    }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_successMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val matchingEtag = "\"${putObjectResult.eTag}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
      .withMatchingETagConstraint(matchingEtag).also {
        s3Client.copyObject(it)
      }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val nonMatchingEtag = "\"${randomName}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withNonmatchingETagConstraint(nonMatchingEtag).also {
        s3Client.copyObject(it)
      }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val nonMatchingEtag = "\"${randomName}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withMatchingETagConstraint(nonMatchingEtag).also {
        s3Client.copyObject(it)
      }

    assertThatThrownBy {
      s3Client.getObject(destinationBucketName, destinationKey)
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey;")
      .hasMessageContaining("The specified key does not exist.")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_failureNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val matchingEtag = "\"${putObjectResult.eTag}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withNonmatchingETagConstraint(matchingEtag).also {
        s3Client.copyObject(it)
      }

    assertThatThrownBy {
      s3Client.getObject(destinationBucketName, destinationKey)
    }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey;")
      .hasMessageContaining("The specified key does not exist.")
  }

  /**
   * Puts an Object; Copies that object to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectToSameKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val objectMetadata = ObjectMetadata().apply {
      this.userMetadata = mapOf("test-key" to "test-value")
    }
    val putObjectResult = PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata).let {
      s3Client.putObject(it)
    }
    //TODO: this is actually illegal on S3. when copying to the same key like this, S3 will throw:
    // This copy request is illegal because it is trying to copy an object to itself without
    // changing the object's metadata, storage class, website redirect location or encryption attributes.
    CopyObjectRequest(bucketName, sourceKey, bucketName, sourceKey).also {
      s3Client.copyObject(it)
    }

    s3Client.getObject(bucketName, sourceKey).use {
      val copiedObjectMetadata = it.objectMetadata
      assertThat(copiedObjectMetadata.userMetadata["test-key"]).isEqualTo("test-value")

      val objectContent = it.objectContent
      val copiedDigest = DigestUtil.hexDigest(objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  /**
   * Puts an Object; Copies that object with REPLACE directive to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithReplaceToSameKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val objectMetadata = ObjectMetadata().apply {
      this.userMetadata = mapOf("test-key" to "test-value")
    }
    val putObjectResult = PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata).let {
      s3Client.putObject(it)
    }

    val replaceObjectMetadata = ObjectMetadata().apply {
      this.userMetadata = mapOf("test-key2" to "test-value2")
    }
    CopyObjectRequest()
      .withSourceBucketName(bucketName)
      .withSourceKey(sourceKey)
      .withDestinationBucketName(bucketName)
      .withDestinationKey(sourceKey)
      .withMetadataDirective(MetadataDirective.REPLACE)
      .withNewObjectMetadata(replaceObjectMetadata)
      .also {
        s3Client.copyObject(it)
      }


    s3Client.getObject(bucketName, sourceKey).use {
      val copiedObjectMetadata = it.objectMetadata
      assertThat(copiedObjectMetadata.userMetadata["test-key"]).isNullOrEmpty()
      assertThat(copiedObjectMetadata.userMetadata["test-key2"]).isEqualTo("test-value2")

      val objectContent = it.objectContent
      val copiedDigest = DigestUtil.hexDigest(objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  /**
   * Puts an Object; Copies that object to a new bucket with new user metadata; Downloads the
   * object from the new bucket;
   * compares checksums of original and copied object; compares copied object user metadata with
   * the new user metadata specified during copy request.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithNewUserMetadata(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"
    val objectMetadata = ObjectMetadata().apply {
      this.addUserMetadata("key", "value")
    }

    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).apply {
      this.newObjectMetadata = objectMetadata
    }.also {
      s3Client.copyObject(it)
    }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
      assertThat(it.objectMetadata.userMetadata).isEqualTo(objectMetadata.userMetadata)
    }
  }

  /**
   * Puts an Object with some user metadata; Copies that object to a new bucket.
   * Downloads the object from the new bucket;
   * compares checksums of original and copied object; compares copied object user metadata with
   * the source object user metadata;
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithSourceUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey/withSourceObjectUserMetadata"
    val sourceObjectMetadata = ObjectMetadata().apply {
      this.addUserMetadata("key", "value")
    }
    val putObjectResult = PutObjectRequest(bucketName, sourceKey, uploadFile).apply {
      this.metadata = sourceObjectMetadata
    }.let {
      s3Client.putObject(it)
    }
    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).also {
      s3Client.copyObject(it)
    }
    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
      assertThat(it.objectMetadata.userMetadata).isEqualTo(sourceObjectMetadata.userMetadata)
    }
  }

  /**
   * Copy an object to a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectToKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/some escape-worthy characters $@ $sourceKey"
    val putObjectResult = s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).also {
      s3Client.copyObject(it)
    }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  /**
   * Copy an object from a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectFromKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = "some escape-worthy characters $@ $UPLOAD_FILE_NAME"
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val putObjectResult = s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).also {
      s3Client.copyObject(it)
    }

    s3Client.getObject(destinationBucketName, destinationKey).use {
      val copiedDigest = DigestUtil.hexDigest(it.objectContent)
      assertThat(copiedDigest).isEqualTo(putObjectResult.eTag)
    }
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  @S3VerifiedFailure(year = 2022,
    reason = "No KMS configuration for AWS test account")
  fun shouldCopyObjectEncrypted(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectResult = CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
      .apply {
        this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
      }.let {
        s3Client.copyObject(it)
      }
    s3Client.getObjectMetadata(destinationBucketName, destinationKey).also {
      assertThat(it.contentLength).isEqualTo(uploadFile.length())
    }

    val uploadDigest = FileInputStream(uploadFile).let {
      DigestUtil.hexDigest(TEST_ENC_KEY_ID, it)
    }
    assertThat(copyObjectResult.eTag).isEqualTo(uploadDigest)
  }

  /**
   * Tests that an object won't be copied with wrong encryption Key.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldNotObjectCopyWithWrongEncryptionKey(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf$sourceKey"
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey).apply {
      this.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
    }

    assertThatThrownBy { s3Client.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests that a copy request for a non-existing object throws the correct error.
   */
  @Test
  @S3VerifiedSuccess(year = 2022)
  fun shouldThrowNoSuchKeyOnCopyForNonExistingKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val sourceKey = randomName
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf$sourceKey"
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)

    assertThatThrownBy { s3Client.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404; Error Code: NoSuchKey")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun multipartCopy() {
    //content larger than default part threshold of 5MiB
    val contentLen = 10 * _1MB
    val objectMetadata = ObjectMetadata().apply {
      this.contentLength = contentLen.toLong()
    }
    val assumedSourceKey = UUID.randomUUID().toString()
    val sourceBucket = givenRandomBucketV1()
    val targetBucket = givenRandomBucketV1()
    val upload = transferManagerV1
      .upload(
        sourceBucket, assumedSourceKey,
        randomInputStream(contentLen), objectMetadata
      )

    val uploadResult = upload.waitForUploadResult().also {
      assertThat(it.key).isEqualTo(assumedSourceKey)
    }

    val assumedDestinationKey = UUID.randomUUID().toString()
    transferManagerV1.copy(
      sourceBucket, assumedSourceKey, targetBucket,
      assumedDestinationKey
    ).waitForCopyResult().also {
      assertThat(it.destinationKey).isEqualTo(assumedDestinationKey)
      assertThat(uploadResult.eTag).isEqualTo(it.eTag)
    }
  }
}
