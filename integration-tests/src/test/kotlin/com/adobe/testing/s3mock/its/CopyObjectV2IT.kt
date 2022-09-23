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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.MetadataDirective
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.File
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CopyObjectV2IT : S3TestBase() {

  @Test
  fun testCopyObject(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    s3ClientV2!!.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .build())

    val copiedObject = s3ClientV2!!.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    )
    val copiedDigest = DigestUtil.hexDigest(copiedObject)
    copiedObject.close()
    assertThat("\"$copiedDigest\"")
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag())
  }

  @Test
  fun testCopyObject_successMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()
    s3ClientV2!!.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .copySourceIfMatch(matchingEtag)
      .build())

    val copiedObject = s3ClientV2!!.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    )
    val copiedDigest = DigestUtil.hexDigest(copiedObject)
    copiedObject.close()
    assertThat("\"$copiedDigest\"")
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(matchingEtag)
  }

  @Test
  fun testCopyObject_successNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""
    s3ClientV2!!.copyObject(CopyObjectRequest
      .builder()
      .sourceBucket(bucketName)
      .sourceKey(sourceKey)
      .destinationBucket(destinationBucketName)
      .destinationKey(destinationKey)
      .copySourceIfNoneMatch(noneMatchingEtag)
      .build())

    val copiedObject = s3ClientV2!!.getObject(GetObjectRequest
      .builder()
      .bucket(destinationBucketName)
      .key(destinationKey)
      .build()
    )
    val copiedDigest = DigestUtil.hexDigest(copiedObject)
    copiedObject.close()
    assertThat("\"$copiedDigest\"")
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag())
  }

  @Test
  fun testCopyObject_failureMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    assertThatThrownBy {
      s3ClientV2!!.copyObject(CopyObjectRequest
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
      .hasMessageContaining("Precondition Failed")
  }

  @Test
  fun testCopyObject_failureNoneMatch(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResult.eTag()
    assertThatThrownBy {
      s3ClientV2!!.copyObject(CopyObjectRequest
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
      .hasMessageContaining("Precondition Failed")
  }

  @Test
  fun testCopyObjectToSameBucketAndKey(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val putObjectResult = s3ClientV2!!.putObject(PutObjectRequest
      .builder()
      .bucket(bucketName)
      .key(sourceKey)
      .metadata(mapOf("test-key" to "test-value"))
      .build(),
      RequestBody.fromFile(uploadFile)
    )
    val headObject = s3ClientV2!!.headObject(
      HeadObjectRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
    )

    val sourceLastModified = headObject.lastModified()

    await("wait until source object is 5 seconds old").until {
      sourceLastModified.plusSeconds(5).isBefore(Instant.now())
    }

    s3ClientV2!!.copyObject(
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

    val responseInputStream =
      s3ClientV2!!.getObject(GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(sourceKey)
        .build()
      )

    val response = responseInputStream.response()
    val copiedObjectMetadata = response.metadata()
    assertThat(copiedObjectMetadata["test-key2"]).isEqualTo("test-value2")
    assertThat(copiedObjectMetadata["test-key"]).isNull()

    val length = response.contentLength()
    assertThat(length).isEqualTo(uploadFile.length())
      .`as`("Copied item must be same length as uploaded file")

    val copiedDigest = DigestUtil.hexDigest(responseInputStream)
    assertThat("\"$copiedDigest\"")
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag())

    //we waited for 5 seconds above, so last modified dates should be about 5 seconds apart
    val between = Duration.between(sourceLastModified, response.lastModified())
    assertThat(between).isCloseTo(Duration.of(5, SECONDS), Duration.of(1, SECONDS))
  }

  @Test
  fun testCopyObjectWithNewMetadata(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObjectV2(testInfo, sourceKey)
    val destinationBucketName = givenRandomBucketV2()
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"

    val metadata = mapOf("test-key2" to "test-value2")
    s3ClientV2!!.copyObject(
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

    val responseInputStream =
      s3ClientV2!!.getObject(GetObjectRequest
        .builder()
        .bucket(destinationBucketName)
        .key(destinationKey)
        .build()
      )

    val copiedDigest = DigestUtil.hexDigest(responseInputStream)
    assertThat("\"$copiedDigest\"")
      .`as`("Source file and copied File should have same digests")
      .isEqualTo(putObjectResult.eTag())
    assertThat(responseInputStream.response().metadata())
      .`as`("User metadata should be identical!")
      .isEqualTo(metadata)
  }
}
