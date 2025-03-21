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

import com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.MetadataDirective
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.StorageClass
import java.io.File
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CopyObjectV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .build())

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject_successMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()
    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .copySourceIfMatch(matchingEtag)
      .build())

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(matchingEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""
    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .copySourceIfNoneMatch(noneMatchingEtag)
      .build())

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    assertThatThrownBy {
      s3ClientV2.copyObject(CopyObjectRequest
        .builder()
        .sourceBucket(bucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destinationBucketName)
        .destinationKey(destinationKey)
        .copySourceIfMatch(noneMatchingEtag)
        .build())
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject_failureNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResult.eTag()

    assertThatThrownBy {
      s3ClientV2.copyObject(CopyObjectRequest
        .builder()
        .sourceBucket(bucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destinationBucketName)
        .destinationKey(destinationKey)
        .copySourceIfNoneMatch(matchingEtag)
        .build())
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObjectToSameBucketAndKey(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val putObjectResult = s3ClientV2.putObject(PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(sourceKey)
      .metadata(mapOf("test-key" to "test-value"))
      .build(),
      RequestBody.fromFile(uploadFile)
    )
    val sourceLastModified = s3ClientV2.headObject(
      HeadObjectRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    ).lastModified()

    await("wait until source object is 5 seconds old").until {
      sourceLastModified.plusSeconds(5).isBefore(Instant.now())
    }

    s3ClientV2.copyObject(
      CopyObjectRequest
        .builder()
        .sourceBucket(bucketName)
        .sourceKey(sourceKey)
        .destinationBucket(bucketName)
        .destinationKey(sourceKey)
        .metadata(mapOf("test-key2" to "test-value2"))
        .metadataDirective(MetadataDirective.REPLACE)
        .build()
    )

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(bucketName)
      .key(sourceKey)
      .build()
    ).use {
      val response = it.response()
      val copiedObjectMetadata = response.metadata()
      assertThat(copiedObjectMetadata["test-key2"]).isEqualTo("test-value2")
      assertThat(copiedObjectMetadata["test-key"]).isNull()

      val length = response.contentLength()
      assertThat(length).isEqualTo(uploadFile.length())

      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())

      //we waited for 5 seconds above, so last modified dates should be about 5 seconds apart
      val between = Duration.between(sourceLastModified, response.lastModified())
      assertThat(between).isCloseTo(Duration.of(5, SECONDS), Duration.of(1, SECONDS))
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObjectToSameBucketAndKey_throws(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    s3ClientV2.putObject(PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(sourceKey)
      .metadata(mapOf("test-key" to "test-value"))
      .build(),
      RequestBody.fromFile(uploadFile)
    )
    val sourceLastModified = s3ClientV2.headObject(
      HeadObjectRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    ).lastModified()

    await("wait until source object is 5 seconds old").until {
      sourceLastModified.plusSeconds(5).isBefore(Instant.now())
    }

    assertThatThrownBy {
      s3ClientV2.copyObject(
        CopyObjectRequest
          .builder()
          .sourceBucket(bucketName)
          .sourceKey(sourceKey)
          .destinationBucket(bucketName)
          .destinationKey(sourceKey)
          .build()
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObjectWithNewMetadata(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"

    val metadata = mapOf("test-key2" to "test-value2")
    s3ClientV2.copyObject(
      CopyObjectRequest
        .builder()
        .sourceBucket(bucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destinationBucketName)
        .destinationKey(destinationKey)
        .metadata(metadata)
        .metadataDirective(MetadataDirective.REPLACE)
        .build()
    )

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
      assertThat(it.response().metadata()).isEqualTo(metadata)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2024)
  fun testCopyObject_storageClass(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(sourceKey)
        .storageClass(StorageClass.REDUCED_REDUNDANCY)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      //must set storage class other than "STANDARD" to it gets applied.
      .storageClass(StorageClass.STANDARD_IA)
      .build())

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      assertThat(it.response().storageClass()).isEqualTo(StorageClass.STANDARD_IA)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testCopyObject_overwriteStoreHeader(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(sourceKey)
        .contentDisposition("")
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .metadataDirective(MetadataDirective.REPLACE)
      .contentDisposition("attachment")
      .build())

    s3ClientV2.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).use {
      assertThat(it.response().contentDisposition()).isEqualTo("attachment")
    }
  }

  @Test
  @S3VerifiedTodo
  fun testCopyObject_encrypted(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    s3ClientV2.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .sseCustomerKey(TEST_ENC_KEY_ID)
      .build()
    )

    s3ClientV2.headObject(HeadObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    ).also {
      assertThat(it.contentLength()).isEqualTo(uploadFile.length())
    }
  }
}
