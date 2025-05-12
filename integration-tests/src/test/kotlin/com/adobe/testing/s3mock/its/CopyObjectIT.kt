/*
 *  Copyright 2017-2025 Adobe.
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
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.MetadataDirective
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import software.amazon.awssdk.services.s3.model.StorageClass
import java.io.File
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Test the application using the AmazonS3 SDK V2.
 */
internal class CopyObjectIT : S3TestBase() {

  private val s3Client: S3Client = createS3Client()
  private val transferManager = createTransferManager()

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with key needing escaping succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = charsSpecialKey()
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val putObjectResult = s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
      }, RequestBody.fromFile(uploadFile)
    )

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-match=true succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.copySourceIfMatch(matchingEtag)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(matchingEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-match=true and if-unmodified-since=false succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val now = Instant.now().minus(60, SECONDS)
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.copySourceIfMatch(matchingEtag)
      it.copySourceIfUnmodifiedSince(now)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(matchingEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-modified-since=true succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val now = Instant.now()
    TimeUnit.SECONDS.sleep(5)
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.copySourceIfModifiedSince(now)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(matchingEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-modified-since=true and if-none-match=false fails`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val now = Instant.now()
    TimeUnit.SECONDS.sleep(5)
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
        it.copySourceIfModifiedSince(now)
        it.copySourceIfNoneMatch(matchingEtag)
      }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-unmodified-since=true succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    TimeUnit.SECONDS.sleep(5)
    val now = Instant.now()
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val matchingEtag = putObjectResult.eTag()

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.copySourceIfUnmodifiedSince(now)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(matchingEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with if-none-match=true succeeds and object can be retrieved`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.copySourceIfNoneMatch(noneMatchingEtag)
    }.copyObjectResult().eTag().also {
      assertThat(it).isEqualTo(putObjectResult.eTag())
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object fails with if-match=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val noneMatchingEtag = "\"${randomName}\""

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
        it.copySourceIfMatch(noneMatchingEtag)
      }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object fails with if-none-match=false`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"
    val matchingEtag = putObjectResult.eTag()

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
        it.copySourceIfNoneMatch(matchingEtag)
      }
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
      .hasMessageContaining(PRECONDITION_FAILED.message)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds with same bucket and key with REPLACE and changing metadata`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val putObjectResult = s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.metadata(mapOf("test-key" to "test-value"))
      },
      RequestBody.fromFile(uploadFile)
    )
    val sourceLastModified = s3Client.headObject {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.lastModified()

    await("wait until source object is 5 seconds old").until {
      sourceLastModified.plusSeconds(5).isBefore(Instant.now())
    }

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(bucketName)
      it.destinationKey(sourceKey)
      it.metadata(mapOf("test-key2" to "test-value2"))
      it.metadataDirective(MetadataDirective.REPLACE)
    }

    s3Client.getObject {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.use {
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
  @S3VerifiedSuccess(year = 2025)
  fun `copy object fails with same bucket and key without changing metadata`(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.metadata(mapOf("test-key" to "test-value"))
      },
      RequestBody.fromFile(uploadFile)
    )

    val sourceLastModified = s3Client.headObject {
      it.bucket(bucketName)
      it.key(sourceKey)
    }.lastModified()

    await("wait until source object is 5 seconds old").until {
      sourceLastModified.plusSeconds(5).isBefore(Instant.now())
    }

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(bucketName)
        it.destinationKey(sourceKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds with source metadata`(testInfo: TestInfo) {

    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)
    val sourceKey = UPLOAD_FILE_NAME
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey/withSourceUserMetadata"

    val metadata = mapOf("test-key2" to "test-value2")

    val putObjectResult = s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.metadata(metadata)
      },
      RequestBody.fromFile(uploadFile)
    )

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
      assertThat(it.response().metadata()).isEqualTo(metadata)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds with new metadata`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResult) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey/withNewUserMetadata"

    val metadata = mapOf("test-key2" to "test-value2")

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.metadata(metadata)
      it.metadataDirective(MetadataDirective.REPLACE)
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      val copiedDigest = DigestUtil.hexDigest(it)
      assertThat("\"$copiedDigest\"").isEqualTo(putObjectResult.eTag())
      assertThat(it.response().metadata()).isEqualTo(metadata)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds with new storageclass`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.storageClass(StorageClass.REDUCED_REDUNDANCY)
      },
      RequestBody.fromFile(uploadFile)
    )

    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
        //must set storage class other than "STANDARD" to it gets applied.
        .storageClass(StorageClass.STANDARD_IA)
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      assertThat(it.response().storageClass()).isEqualTo(StorageClass.STANDARD_IA)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object succeeds with overwriting stored headers`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(sourceKey)
        it.contentDisposition("")
      },
      RequestBody.fromFile(uploadFile)
    )

    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.metadataDirective(MetadataDirective.REPLACE)
      it.contentDisposition("attachment")
    }

    s3Client.getObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.use {
      assertThat(it.response().contentDisposition()).isEqualTo("attachment")
    }
  }

  @Test
  @S3VerifiedFailure(year = 2025,
    reason = "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm")
  fun `copy object succeeds with encryption`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    s3Client.copyObject {
      it.sourceBucket(bucketName)
      it.sourceKey(sourceKey)
      it.destinationBucket(destinationBucketName)
      it.destinationKey(destinationKey)
      it.sseCustomerKey(TEST_ENC_KEY_ID)
    }

    s3Client.headObject {
      it.bucket(destinationBucketName)
      it.key(destinationKey)
    }.also {
      assertThat(it.eTag()).isEqualTo(putObjectResponse.eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object fails with wrong encryption key`(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, sourceKey)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
        it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
        it.ssekmsKeyId(TEST_WRONG_KEY_ID)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Invalid keyId 'key-ID-WRONGWRONGWRONG'")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object fails with non existing source key`(testInfo: TestInfo) {
    val sourceKey = randomName
    val bucketName = givenBucket(testInfo)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
      .hasMessageContaining("The specified key does not exist.")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `copy object with transfermanager succeeds`(testInfo: TestInfo) {
    //content larger than default part threshold of 8MiB
    val contentLen = 20 * _1MB
    val sourceKey = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)
    val destinationBucketName = givenBucket()
    val destinationKey = "copyOf/$sourceKey"

    val upload = transferManager.upload {
      it.putObjectRequest {
        it.key(sourceKey)
        it.bucket(bucketName)
      }
      it.requestBody(AsyncRequestBody.fromInputStream(randomInputStream(contentLen),
        contentLen.toLong(),
        Executors.newFixedThreadPool(10)))
    }.completionFuture().join()

    transferManager.copy {
      it.copyObjectRequest {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
      }
    }.completionFuture().join().also {
      assertThat(it.response().copyObjectResult().eTag()).isEqualTo(upload.response().eTag())
    }
  }
}
