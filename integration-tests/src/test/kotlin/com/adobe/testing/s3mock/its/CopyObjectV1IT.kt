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

import com.adobe.testing.s3mock.util.DigestUtil
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.MetadataDirective
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
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

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObject(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObjectToSameKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest =
      PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, bucketName, sourceKey)

    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(bucketName, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"]).isEqualTo("test-value")

    val objectContent = copiedObject.objectContent
    val length = objectContent.available()
    assertThat(length).isEqualTo(uploadFile.length())
      .`as`("Copied item must be same length as uploaded file")

    val copiedDigest = DigestUtil.hexDigest(objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object with REPLACE directive to the same bucket and the same key;
   * Downloads the object; compares checksums of original and copied object.
   */
  @Test
  fun shouldCopyObjectWithReplaceToSameKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest =
      PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val replaceObjectMetadata = ObjectMetadata()
    replaceObjectMetadata.userMetadata = mapOf("test-key2" to "test-value2")
    val copyObjectRequest = CopyObjectRequest()
      .withSourceBucketName(bucketName)
      .withSourceKey(sourceKey)
      .withDestinationBucketName(bucketName)
      .withDestinationKey(sourceKey)
      .withMetadataDirective(MetadataDirective.REPLACE)
      .withNewObjectMetadata(replaceObjectMetadata)

    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(bucketName, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"])
      .`as`("Original userMetadata must have been replaced.")
      .isNullOrEmpty()
    assertThat(copiedObjectMetadata.userMetadata["test-key2"]).isEqualTo("test-value2")

    val objectContent = copiedObject.objectContent
    val length = objectContent.available()
    assertThat(length).isEqualTo(uploadFile.length())
      .`as`("Copied item must be same length as uploaded file")

    val copiedDigest = DigestUtil.hexDigest(objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to a new bucket with new user metadata; Downloads the
   * object from the new bucket;
   * compares checksums of original and copied object; compares copied object user metadata with
   * the new user metadata specified during copy request.
   */
  @Test
  fun shouldCopyObjectWithNewUserMetadata(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"
    s3Client!!.createBucket(destinationBucketName)
    val objectMetadata = ObjectMetadata()
    objectMetadata.addUserMetadata("key", "value")
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.newObjectMetadata = objectMetadata
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
    assertThat(copiedObject.objectMetadata.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(objectMetadata.userMetadata)
  }

  /**
   * Puts an Object with some user metadata; Copies that object to a new bucket.
   * Downloads the object from the new bucket;
   * compares checksums of original and copied object; compares copied object user metadata with
   * the source object user metadata;
   */
  @Test
  fun shouldCopyObjectWithSourceUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey/withSourceObjectUserMetadata"
    s3Client!!.createBucket(destinationBucketName)
    val sourceObjectMetadata = ObjectMetadata()
    sourceObjectMetadata.addUserMetadata("key", "value")
    val putObjectRequest = PutObjectRequest(bucketName, sourceKey, uploadFile)
    putObjectRequest.metadata = sourceObjectMetadata
    val putObjectResult = s3Client!!.putObject(putObjectRequest)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
    assertThat(copiedObject.objectMetadata.userMetadata)
      .`as`("User metadata should be identical!")
      .isEqualTo(sourceObjectMetadata.userMetadata)
  }

  /**
   * Copy an object to a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  fun shouldCopyObjectToKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = randomName
    val destinationKey = "copyOf/some escape-worthy characters %$@ $sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Copy an object from a key needing URL escaping.
   *
   * @see .shouldCopyObject
   */
  @Test
  fun shouldCopyObjectFromKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = "some escape-worthy characters %$@ $UPLOAD_FILE_NAME"
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val putObjectResult = s3Client!!.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client!!.copyObject(copyObjectRequest)
    val copiedObject = s3Client!!.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  /**
   * Puts an Object; Copies that object to a new bucket; Downloads the object from the new bucket;
   * compares checksums of original and copied object.
   */
  @Test
  @Throws(Exception::class)
  fun shouldCopyObjectEncrypted(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3Client!!.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    val copyObjectResult = s3Client!!.copyObject(copyObjectRequest)
    val metadata = s3Client!!.getObjectMetadata(destinationBucketName, destinationKey)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val uploadDigest = DigestUtil.hexDigest(TEST_ENC_KEY_ID, uploadFileIs)
    assertThat(copyObjectResult.eTag)
      .`as`("ETag should match")
      .isEqualTo(uploadDigest)
    assertThat(metadata.contentLength)
      .`as`("Files should have the same length")
      .isEqualTo(uploadFile.length())
  }

  /**
   * Tests that an object won't be copied with wrong encryption Key.
   */
  @Test
  fun shouldNotObjectCopyWithWrongEncryptionKey(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = randomName
    val destinationKey = "copyOf$sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
    Assertions.assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 400; Error Code: KMS.NotFoundException")
  }

  /**
   * Tests that a copy request for a non-existing object throws the correct error.
   */
  @Test
  fun shouldThrowNoSuchKeyOnCopyForNonExistingKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val sourceKey = randomName
    val destinationBucketName = randomName
    val destinationKey = "copyOf$sourceKey"
    s3Client!!.createBucket(destinationBucketName)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    Assertions.assertThatThrownBy { s3Client!!.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404; Error Code: NoSuchKey")
  }

  @Test
  fun multipartCopy() {
    //content larger than default part threshold of 5MiB
    val contentLen = 10 * _1MB
    val objectMetadata = ObjectMetadata()
    objectMetadata.contentLength = contentLen.toLong()
    val assumedSourceKey = UUID.randomUUID().toString()
    val sourceBucket = givenRandomBucketV1()
    val targetBucket = givenRandomBucketV1()
    val transferManager = createTransferManager()
    val upload = transferManager
      .upload(
        sourceBucket, assumedSourceKey,
        randomInputStream(contentLen), objectMetadata
      )
    val uploadResult = upload.waitForUploadResult()
    assertThat(uploadResult.key).isEqualTo(assumedSourceKey)
    val assumedDestinationKey = UUID.randomUUID().toString()
    val copy = transferManager.copy(
      sourceBucket, assumedSourceKey, targetBucket,
      assumedDestinationKey
    )
    val copyResult = copy.waitForCopyResult()
    assertThat(copyResult.destinationKey).isEqualTo(assumedDestinationKey)
    assertThat(uploadResult.eTag)
      .`as`("Hashes for source and target S3Object do not match.")
      .isEqualTo(copyResult.eTag)
  }
}
