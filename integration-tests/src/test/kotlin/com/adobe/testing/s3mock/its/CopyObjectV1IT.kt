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

import com.adobe.testing.s3mock.util.DigestUtil
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CopyObjectRequest
import com.amazonaws.services.s3.model.MetadataDirective
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
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
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_successMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val matchingEtag = "\"${putObjectResult.eTag}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withMatchingETagConstraint(matchingEtag)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val nonMatchingEtag = "\"${randomName}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withNonmatchingETagConstraint(nonMatchingEtag)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
    val copiedDigest = DigestUtil.hexDigest(copiedObject.objectContent)
    copiedObject.close()
    assertThat(copiedDigest)
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testCopyObject_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val nonMatchingEtag = "\"${randomName}\""
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withMatchingETagConstraint(nonMatchingEtag)
    s3Client.copyObject(copyObjectRequest)

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
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
        .withNonmatchingETagConstraint(matchingEtag)
    s3Client.copyObject(copyObjectRequest)

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
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest =
      PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client.putObject(putObjectRequest)
    //TODO: this is actually illegal on S3. when copying to the same key like this, S3 will throw:
    // This copy request is illegal because it is trying to copy an object to itself without
    // changing the object's metadata, storage class, website redirect location or encryption attributes.
    val copyObjectRequest = CopyObjectRequest(bucketName, sourceKey, bucketName, sourceKey)

    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(bucketName, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"]).isEqualTo("test-value")

    val objectContent = copiedObject.objectContent
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithReplaceToSameKey(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val objectMetadata = ObjectMetadata()
    objectMetadata.userMetadata = mapOf("test-key" to "test-value")
    val putObjectRequest =
      PutObjectRequest(bucketName, sourceKey, uploadFile).withMetadata(objectMetadata)
    val putObjectResult = s3Client.putObject(putObjectRequest)
    val replaceObjectMetadata = ObjectMetadata()
    replaceObjectMetadata.userMetadata = mapOf("test-key2" to "test-value2")
    val copyObjectRequest = CopyObjectRequest()
      .withSourceBucketName(bucketName)
      .withSourceKey(sourceKey)
      .withDestinationBucketName(bucketName)
      .withDestinationKey(sourceKey)
      .withMetadataDirective(MetadataDirective.REPLACE)
      .withNewObjectMetadata(replaceObjectMetadata)

    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(bucketName, sourceKey)
    val copiedObjectMetadata = copiedObject.objectMetadata
    assertThat(copiedObjectMetadata.userMetadata["test-key"])
      .`as`("Original userMetadata must have been replaced.")
      .isNullOrEmpty()
    assertThat(copiedObjectMetadata.userMetadata["test-key2"]).isEqualTo("test-value2")

    val objectContent = copiedObject.objectContent
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithNewUserMetadata(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"
    val objectMetadata = ObjectMetadata()
    objectMetadata.addUserMetadata("key", "value")
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.newObjectMetadata = objectMetadata
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectWithSourceUserMetadata(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey/withSourceObjectUserMetadata"
    val sourceObjectMetadata = ObjectMetadata()
    sourceObjectMetadata.addUserMetadata("key", "value")
    val putObjectRequest = PutObjectRequest(bucketName, sourceKey, uploadFile)
    putObjectRequest.metadata = sourceObjectMetadata
    val putObjectResult = s3Client.putObject(putObjectRequest)
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectToKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/some escape-worthy characters $@ $sourceKey"
    val putObjectResult = s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldCopyObjectFromKeyNeedingEscaping(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = "some escape-worthy characters $@ $UPLOAD_FILE_NAME"
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val putObjectResult = s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    s3Client.copyObject(copyObjectRequest)
    val copiedObject = s3Client.getObject(destinationBucketName, destinationKey)
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
  @S3VerifiedFailure(year = 2022,
    reason = "No KMS configuration for AWS test account")
  fun shouldCopyObjectEncrypted(testInfo: TestInfo) {
    val bucketName = givenBucketV1(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3Client.putObject(PutObjectRequest(bucketName, sourceKey, uploadFile))
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf/$sourceKey"
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_ENC_KEY_ID)
    val copyObjectResult = s3Client.copyObject(copyObjectRequest)
    val metadata = s3Client.getObjectMetadata(destinationBucketName, destinationKey)
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
  @S3VerifiedSuccess(year = 2022)
  fun shouldNotObjectCopyWithWrongEncryptionKey(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV1(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV1()
    val destinationKey = "copyOf$sourceKey"
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    copyObjectRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(TEST_WRONG_KEY_ID)
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
    val copyObjectRequest =
      CopyObjectRequest(bucketName, sourceKey, destinationBucketName, destinationKey)
    assertThatThrownBy { s3Client.copyObject(copyObjectRequest) }
      .isInstanceOf(AmazonS3Exception::class.java)
      .hasMessageContaining("Status Code: 404; Error Code: NoSuchKey")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
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
