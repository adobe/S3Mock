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

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.http.ContentDisposition
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ChecksumMode
import software.amazon.awssdk.services.s3.model.ChecksumType
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import software.amazon.awssdk.services.s3.model.StorageClass
import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.math.min

internal class GetPutDeleteObjectIT : S3TestBase() {
  private val s3Client: S3Client = createS3Client()
  private val s3ClientHttp: S3Client = createS3Client(serviceEndpointHttp)
  private val s3AsyncClient: S3AsyncClient = createS3AsyncClient()
  private val s3AsyncClientHttp: S3AsyncClient = createS3AsyncClient(serviceEndpointHttp)
  private val s3CrtAsyncClient: S3AsyncClient = createS3CrtAsyncClient()
  private val s3CrtAsyncClientHttp: S3AsyncClient = createS3CrtAsyncClient(serviceEndpointHttp)
  private val autoS3CrtAsyncClient: S3AsyncClient = createAutoS3CrtAsyncClient()
  private val autoS3CrtAsyncClientHttp: S3AsyncClient = createAutoS3CrtAsyncClient(serviceEndpointHttp)

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadDeleteObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(key)
    }

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(key)
      }
    }.isInstanceOf(NoSuchKeyException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadDeleteObject_pathSegments(testInfo: TestInfo) {
    val key = "/.././$UPLOAD_FILE_NAME"
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(key)
    }

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(key)
      }
    }.isInstanceOf(NoSuchKeyException::class.java)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadDeleteObjects(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)
    val keys = listOf("$key-1", "$key-2", "$key-3")
    keys.forEach { key ->
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(key)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }

    s3Client.deleteObjects {
      it.bucket(bucketName)
      it.delete {
        it.objects(
          { it.key("$key-1") },
          { it.key("$key-2") },
          { it.key("$key-3") },
        )
      }
    }

    keys.forEach { key ->
      assertThatThrownBy {
        s3Client.getObject {
          it.bucket(bucketName)
          it.key(key)
        }
      }.isInstanceOf(NoSuchKeyException::class.java)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun getObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(NON_EXISTING_KEY)
      }
    }.isInstanceOf(
      NoSuchKeyException::class.java,
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun getObject_noSuchKey_startingSlash(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key("/$NON_EXISTING_KEY")
      }
    }.isInstanceOf(
      NoSuchKeyException::class.java,
    ).hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun putObject_noSuchBucket() {
    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun putObjectEncrypted_noSuchBucket() {
    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(randomName)
          it.key(UPLOAD_FILE_NAME)
          it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
          it.ssekmsKeyId(TEST_ENC_KEY_ID)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun headObject_noSuchBucket() {
    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(randomName)
        it.key(UPLOAD_FILE_NAME)
      }
    }
      // TODO: not sure why AWS SDK v2 does not return the correct exception here, S3Mock returns the correct error message.
      .isInstanceOf(NoSuchKeyException::class.java)
    // .isInstanceOf(NoSuchBucketException::class.java)
    // .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun headObject_noSuchKey(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(NON_EXISTING_KEY)
      }
    }.isInstanceOf(NoSuchKeyException::class.java)
    // TODO: not sure why AWS SDK v2 does not return the correct error message, S3Mock returns the correct message.
    // .hasMessageContaining(NO_SUCH_KEY)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun copyObjectToNonExistingDestination_noSuchBucket(testInfo: TestInfo) {
    val sourceKey = UPLOAD_FILE_NAME
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val destinationBucketName = randomName
    val destinationKey = "copyOf/$sourceKey"

    assertThatThrownBy {
      s3Client.copyObject {
        it.sourceBucket(bucketName)
        it.sourceKey(sourceKey)
        it.destinationBucket(destinationBucketName)
        it.destinationKey(destinationKey)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun deleteObject_noSuchBucket() {
    assertThatThrownBy {
      s3Client.deleteObject {
        it.bucket(randomName)
        it.key(NON_EXISTING_KEY)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun deleteObject_nonExistent_OK(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(NON_EXISTING_KEY)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun deleteObjects_noSuchBucket() {
    assertThatThrownBy {
      s3Client.deleteObjects {
        it.bucket(randomName)
        it.delete {
          it.objects({
            it.key(NON_EXISTING_KEY)
          })
        }
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun deleteBucket_noSuchBucket() {
    assertThatThrownBy {
      s3Client.deleteBucket {
        it.bucket(randomName)
      }
    }.isInstanceOf(NoSuchBucketException::class.java)
      .hasMessageContaining(NO_SUCH_BUCKET)
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadDeleteObjects_nonExistentKey(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)
    givenObject(bucketName, key)

    s3Client.deleteObjects {
      it.bucket(bucketName)
      it.delete {
        it.objects(
          { it.key(key) },
          { it.key(randomName) },
        )
      }
    }
  }

  /**
   * Test safe characters in object keys
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["charsSafe", "charsSpecial", "charsToAvoid"])
  fun testPutHeadGetObject_keyNames_safe(
    key: String,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["storageClasses"])
  fun testPutObject_storageClass(
    storageClass: StorageClass,
    testInfo: TestInfo,
  ) {
    val bucketName = givenBucket(testInfo)

    val key = UPLOAD_FILE_NAME

    val eTag =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(key)
            it.storageClass(storageClass)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(key)
      }.also {
        assertThat(it.eTag()).isEqualTo(eTag)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(eTag)
        if (storageClass == StorageClass.STANDARD) {
          // storageClass STANDARD is never returned from S3 APIs...
          assertThat(it.response().storageClass()).isNull()
        } else {
          assertThat(it.response().storageClass()).isEqualTo(storageClass)
        }
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["testFileNames"])
  fun testPutObject_etagCreation_sync(
    testFileName: String,
    testInfo: TestInfo,
  ) {
    testEtagCreation(testFileName, s3Client, testInfo)
    testEtagCreation(testFileName, s3ClientHttp, testInfo)
  }

  private fun testEtagCreation(
    testFileName: String,
    s3Client: S3Client,
    testInfo: TestInfo,
  ) {
    val uploadFile = File(testFileName)
    val expectedEtag =
      uploadFile.inputStream().use {
        "\"${DigestUtil.hexDigest(it)}\""
      }
    val bucketName = givenBucket(testInfo)
    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(testFileName)
        },
        RequestBody.fromFile(uploadFile),
      ).eTag()
      .also {
        assertThat(it).isNotBlank
        assertThat(it).isEqualTo(expectedEtag)
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["testFileNames"])
  fun testPutObject_etagCreation_async(testFileName: String) {
    testEtagCreation(testFileName, s3AsyncClient)
    testEtagCreation(testFileName, s3AsyncClientHttp)
    testEtagCreation(testFileName, s3CrtAsyncClient)
    testEtagCreation(testFileName, s3CrtAsyncClientHttp)
    testEtagCreation(testFileName, autoS3CrtAsyncClient)
    testEtagCreation(testFileName, autoS3CrtAsyncClientHttp)
  }

  private fun testEtagCreation(
    testFileName: String,
    s3Client: S3AsyncClient,
  ) {
    val uploadFile = File(testFileName)
    val expectedEtag =
      uploadFile.inputStream().use {
        "\"${DigestUtil.hexDigest(it)}\""
      }
    val bucketName = givenBucket(randomName)
    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(testFileName)
        },
        AsyncRequestBody.fromFile(uploadFile),
      ).join()
      .eTag()
      .also {
        assertThat(it).isNotBlank
        assertThat(it).isEqualTo(expectedEtag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PutObject and getObjectAttributes succeeds`(testInfo: TestInfo) {
    val expectedChecksum = DigestUtil.checksumFor(UPLOAD_FILE_PATH, DefaultChecksumAlgorithm.SHA1)
    val bucketName = givenBucket(testInfo)

    val eTag =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(UPLOAD_FILE_NAME)
            it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    s3Client
      .getObjectAttributes {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.objectAttributes(
          ObjectAttributes.OBJECT_SIZE,
          ObjectAttributes.STORAGE_CLASS,
          ObjectAttributes.E_TAG,
          ObjectAttributes.CHECKSUM,
        )
      }.also {
        assertThat(it.eTag()).isEqualTo(eTag.trim('"'))
        // GetObjectAttributes returns the default storageClass "STANDARD", even though other APIs may not.
        assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
        assertThat(it.objectSize()).isEqualTo(UPLOAD_FILE_LENGTH)
        assertThat(it.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
        assertThat(it.checksum().checksumType()).isEqualTo(ChecksumType.FULL_OBJECT)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_objectMetadata(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.metadata(mapOf("key1" to "value1", "key2" to "value2"))
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).eTag()

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.let {
        assertThat(it.response().metadata()).containsAllEntriesOf(mapOf("key1" to "value1", "key2" to "value2"))
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_http(checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm) {
    testChecksumAlgorithm(SAMPLE_FILE, checksumAlgorithm, s3ClientHttp)
    testChecksumAlgorithm(SAMPLE_FILE_LARGE, checksumAlgorithm, s3ClientHttp)
    testChecksumAlgorithm(TEST_IMAGE, checksumAlgorithm, s3ClientHttp)
    testChecksumAlgorithm(TEST_IMAGE_LARGE, checksumAlgorithm, s3ClientHttp)
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_https(checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm) {
    testChecksumAlgorithm(SAMPLE_FILE, checksumAlgorithm, s3Client)
    testChecksumAlgorithm(SAMPLE_FILE_LARGE, checksumAlgorithm, s3Client)
    testChecksumAlgorithm(TEST_IMAGE, checksumAlgorithm, s3Client)
    testChecksumAlgorithm(TEST_IMAGE_LARGE, checksumAlgorithm, s3Client)
  }

  private fun testChecksumAlgorithm(
    testFileName: String,
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    s3Client: S3Client,
  ) {
    val uploadFile = File(testFileName)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm)
    val bucketName = givenBucket(randomName)

    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(testFileName)
          it.checksumAlgorithm(checksumAlgorithm.toAlgorithm())
        },
        RequestBody.fromFile(uploadFile),
      ).also {
        val putChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        val getChecksum = it.response().checksum(checksumAlgorithm.toAlgorithm())
        assertThat(getChecksum).isNotBlank
        assertThat(getChecksum).isEqualTo(expectedChecksum)
      }

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumMode(ChecksumMode.ENABLED)
      }.also {
        val headChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(headChecksum).isNotBlank
        assertThat(headChecksum).isEqualTo(expectedChecksum)
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_async_http(checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm) {
    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3AsyncClientHttp)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3AsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3AsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3AsyncClientHttp)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3CrtAsyncClientHttp)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, autoS3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, autoS3CrtAsyncClientHttp)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientHttp)
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_async_https(checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm) {
    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3AsyncClient)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3AsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3AsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3AsyncClient)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3CrtAsyncClient)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3CrtAsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3CrtAsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3CrtAsyncClient)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, autoS3CrtAsyncClient)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, autoS3CrtAsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, autoS3CrtAsyncClient)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, autoS3CrtAsyncClient)
  }

  private fun testChecksumAlgorithm_async(
    testFileName: String,
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    s3Client: S3AsyncClient,
  ) {
    val uploadFile = File(testFileName)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm)
    val bucketName = givenBucket(randomName)

    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(testFileName)
          it.checksumAlgorithm(checksumAlgorithm.toAlgorithm())
        },
        AsyncRequestBody.fromFile(uploadFile),
      ).join()
      .also {
        val putChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)
      }

    this.s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        val getChecksum = it.response().checksum(checksumAlgorithm.toAlgorithm())
        assertThat(getChecksum).isNotBlank
        assertThat(getChecksum).isEqualTo(expectedChecksum)
      }

    this.s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumMode(ChecksumMode.ENABLED)
      }.also {
        val headChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(headChecksum).isNotBlank
        assertThat(headChecksum).isEqualTo(expectedChecksum)
      }
  }

  private fun PutObjectRequest.Builder.checksum(
    checksum: String,
    checksumAlgorithm: ChecksumAlgorithm,
  ): PutObjectRequest.Builder =
    when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> checksumSHA1(checksum)
      ChecksumAlgorithm.SHA256 -> checksumSHA256(checksum)
      ChecksumAlgorithm.CRC32 -> checksumCRC32(checksum)
      ChecksumAlgorithm.CRC32_C -> checksumCRC32C(checksum)
      ChecksumAlgorithm.CRC64_NVME -> checksumCRC64NVME(checksum)
      else -> error("Unknown checksum algorithm")
    }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksum(
    checksumAlgorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm,
    testInfo: TestInfo,
  ) {
    val expectedChecksum = DigestUtil.checksumFor(UPLOAD_FILE_PATH, checksumAlgorithm)
    val bucketName = givenBucket(testInfo)

    s3Client
      .putObject(
        {
          it.checksum(expectedChecksum, checksumAlgorithm.toAlgorithm())
          it.bucket(bucketName).key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).also {
        val putChecksum = it.checksum(checksumAlgorithm.toAlgorithm())!!
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.checksumMode(ChecksumMode.ENABLED)
      }.use {
        val getChecksum = it.response().checksum(checksumAlgorithm.toAlgorithm())
        assertThat(getChecksum).isNotBlank
        assertThat(getChecksum).isEqualTo(expectedChecksum)
      }

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.checksumMode(ChecksumMode.ENABLED)
      }.also {
        val headChecksum = it.checksum(checksumAlgorithm.toAlgorithm())
        assertThat(headChecksum).isNotBlank
        assertThat(headChecksum).isEqualTo(expectedChecksum)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_wrongChecksum(testInfo: TestInfo) {
    val expectedChecksum = "wrongChecksum"
    val checksumAlgorithm = ChecksumAlgorithm.SHA1
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.checksum(expectedChecksum, checksumAlgorithm)
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Value for x-amz-checksum-sha1 header is invalid.")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_wrongEncryptionKey(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.ssekmsKeyId(TEST_WRONG_KEY_ID)
          it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Invalid keyId 'key-ID-WRONGWRONGWRONG'")
  }

  /**
   * Safe characters:
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_safeCharacters(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val key = "someKey${charsSafeKey()}"

    val eTag =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(key)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(key)
      }.also {
        assertThat(it.eTag()).isEqualTo(eTag)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(eTag).isEqualTo(it.response().eTag())
      }
  }

  /**
   * Characters needing special handling:
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_specialHandlingCharacters(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val key = "someKey${charsSpecialKey()}"

    val eTag =
      s3Client
        .putObject(
          {
            it.bucket(bucketName)
            it.key(key)
          },
          RequestBody.fromFile(UPLOAD_FILE),
        ).eTag()

    s3Client
      .headObject(
        HeadObjectRequest
          .builder()
          .bucket(bucketName)
          .key(key)
          .build(),
      ).also {
        assertThat(it.eTag()).isEqualTo(eTag)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(eTag).isEqualTo(it.response().eTag())
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetDeleteObject_twoBuckets() {
    val bucket1 = givenBucket()
    val bucket2 = givenBucket()
    givenObject(bucket1, UPLOAD_FILE_NAME)
    givenObject(bucket2, UPLOAD_FILE_NAME)
    getObject(bucket1, UPLOAD_FILE_NAME).use {}

    deleteObject(bucket1, UPLOAD_FILE_NAME)
    assertThatThrownBy {
      getObject(bucket1, UPLOAD_FILE_NAME)
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")

    getObject(bucket2, UPLOAD_FILE_NAME).use {
      assertThat(getObject(bucket2, UPLOAD_FILE_NAME).use { it.response().eTag() }).isEqualTo(it.response().eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadObject_storeHeaders() {
    val bucket = givenBucket()
    val contentDisposition =
      ContentDisposition
        .formData()
        .name("file")
        .filename("sampleFile.txt")
        .build()
        .toString()
    val expires = Instant.now()
    val encoding = "SomeEncoding"
    val contentLanguage = "SomeLanguage"
    val cacheControl = "SomeCacheControl"

    s3Client.putObject(
      {
        it.bucket(bucket)
        it.key(UPLOAD_FILE_NAME)
        it.contentDisposition(contentDisposition)
        it.contentEncoding(encoding)
        it.expires(expires)
        it.contentLanguage(contentLanguage)
        it.cacheControl(cacheControl)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    getObject(bucket, UPLOAD_FILE_NAME).use {
      assertThat(it.response().contentDisposition()).isEqualTo(contentDisposition)
      assertThat(it.response().contentEncoding()).isEqualTo(encoding)
      // time in second precision, see
      // https://www.rfc-editor.org/rfc/rfc7234#section-5.3
      // https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
      assertThat(it.response().expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
      assertThat(it.response().contentLanguage()).isEqualTo(contentLanguage)
      assertThat(it.response().cacheControl()).isEqualTo(cacheControl)
    }

    s3Client
      .headObject {
        it.bucket(bucket)
        it.key(UPLOAD_FILE_NAME)
      }.also {
        assertThat(it.contentDisposition()).isEqualTo(contentDisposition)
        assertThat(it.contentEncoding()).isEqualTo(encoding)
        assertThat(it.expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
        assertThat(it.contentLanguage()).isEqualTo(contentLanguage)
        assertThat(it.cacheControl()).isEqualTo(cacheControl)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT object succeeds with if-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(matchingEtag)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT object fails with if-none-match=false with wildcard`(testInfo: TestInfo) {
    val nonMatchingEtag = WILDCARD
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.ifNoneMatch(nonMatchingEtag)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT object succeeds with if-none-match on non-existing object`(testInfo: TestInfo) {
    val nonMatchingEtag = WILDCARD
    val bucketName = givenBucket(testInfo)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(nonMatchingEtag)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT object fails with if-match=false`(testInfo: TestInfo) {
    val nonMatchingEtag = "\"$randomName\""
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.ifMatch(nonMatchingEtag)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `PUT object fails with if-match on non-existing object`(testInfo: TestInfo) {
    val nonMatchingEtag = "\"$randomName\""
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3Client.putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.ifMatch(nonMatchingEtag)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object succeeds with if-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(expectedEtag)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object succeeds with if-match=true with wildcard`(testInfo: TestInfo) {
    val matchingEtag = WILDCARD
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(matchingEtag)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object succeeds with if-match-size=true`(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatchSize(UPLOAD_FILE_LENGTH)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object succeeds with if-match-last-modified-time=true`(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    val lastModified =
      s3Client
        .headObject {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        }.lastModified()

    s3Client.deleteObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatchLastModifiedTime(lastModified)
    }
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object fails with if-match=false`(testInfo: TestInfo) {
    val nonMatchingEtag = "\"$randomName\""
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.deleteObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object fails with if-match-size=false`(testInfo: TestInfo) {
    val nonMatchingSize = 0L
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.deleteObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatchSize(nonMatchingSize)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedFailure(
    year = 2025,
    reason = "Supported only on directory buckets. S3 returns: A header you provided implies functionality that is not implemented.",
  )
  fun `DELETE object fails with if-match-last-modified-time=false`(testInfo: TestInfo) {
    val lastModifiedTime = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.deleteObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatchLastModifiedTime(lastModifiedTime)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object succeeds with if-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(expectedEtag)
      }.also {
        assertThat(it.eTag()).isEqualTo(expectedEtag)
      }
  }

  @Disabled(
    "Spring Boot sends a 412 for this request even though the controller returns a 200 OK." +
      "This test succeeds against the AWS S3 API.",
  )
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object succeeds with if-match=true and if-unmodified-since=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(expectedEtag)
        it.ifUnmodifiedSince(now)
      }.also {
        assertThat(it.eTag()).isEqualTo(expectedEtag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object fails with if-match=false`(testInfo: TestInfo) {
    val nonMatchingEtag = "\"$randomName\""
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object succeeds with if-none-match=true`(testInfo: TestInfo) {
    val nonMatchingEtag = "\"$randomName\""
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(nonMatchingEtag)
      }.also {
        assertThat(it.eTag()).isEqualTo(expectedEtag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object fails with if-none-match=false with wildcard`(testInfo: TestInfo) {
    val nonMatchingEtag = WILDCARD
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object fails with if-modified-since=true and if-none-match=false with wildcard`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val nonMatchingEtag = WILDCARD
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
        it.ifNoneMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object succeeds with if-modified-since=true`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
      }.also {
        assertThat(it.eTag()).isEqualTo(expectedEtag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object fails with if-modified-since=false`(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val now = Instant.now().plusSeconds(60)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object succeeds with if-unmodified-since=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val expectedEtag = putObjectResponse.eTag()
    val now = Instant.now().plusSeconds(60)

    s3Client
      .headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifUnmodifiedSince(now)
      }.also {
        assertThat(it.eTag()).isEqualTo(expectedEtag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `HEAD object fails with if-unmodified-since=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifUnmodifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(matchingEtag)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(matchingEtag)
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  @Test
  @S3VerifiedTodo
  fun `GET object succeeds with unquoted if-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()
    val unquotedEtag = matchingEtag.substring(1, matchingEtag.length - 1)
    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(unquotedEtag)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(matchingEtag)
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-match=true and if-unmodified-since=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(matchingEtag)
        it.ifUnmodifiedSince(now)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(matchingEtag)
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-match=true with wildcard`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val matchingEtag = WILDCARD

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(matchingEtag)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(eTag)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-none-match=true`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()

    val noneMatchingEtag = "\"$randomName\""

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(noneMatchingEtag)
      }.use {
        assertThat(it.response().eTag()).isEqualTo(matchingEtag)
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object fails with if-none-match=false`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(matchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedTodo
  fun `GET object fails with unquoted if-none-match=false`(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val matchingEtag = putObjectResponse.eTag()
    val unquotedEtag = matchingEtag.substring(1, matchingEtag.length - 1)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(unquotedEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-modified-since=true`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
      }.use {
        assertThat(it.response().eTag()).isNotNull()
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object fails with if-modified-since=true and if-none-match=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
        it.ifNoneMatch(putObjectResponse.eTag())
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object fails with if-modified-since=false`(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val now = Instant.now().plusSeconds(60)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object succeeds with if-unmodified-since=true`(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val now = Instant.now().plusSeconds(60)

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifUnmodifiedSince(now)
      }.use {
        assertThat(it.response().eTag()).isNotNull()
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun `GET object fails with if-unmodified-since=false`(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifUnmodifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val smallRequestStartBytes = 1L
    val smallRequestEndBytes = 2L

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(eTag)
        it.range("bytes=$smallRequestStartBytes-$smallRequestEndBytes")
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(smallRequestEndBytes)
        assertThat(it.response().contentRange())
          .isEqualTo("bytes $smallRequestStartBytes-$smallRequestEndBytes/$UPLOAD_FILE_LENGTH")
      }

    val largeRequestStartBytes = 0L
    val largeRequestEndBytes = 1000L

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.range("bytes=$largeRequestStartBytes-$largeRequestEndBytes")
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(min(UPLOAD_FILE_LENGTH, largeRequestEndBytes + 1))
        assertThat(it.response().contentRange())
          .isEqualTo(
            "bytes $largeRequestStartBytes-${min(UPLOAD_FILE_LENGTH - 1, largeRequestEndBytes)}/$UPLOAD_FILE_LENGTH",
          )
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads_fail_emptyObject(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      },
      RequestBody.fromBytes(ByteArray(0)),
    )
    val smallRequestEndBytes = 2L

    assertThatThrownBy {
      s3Client.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.range("bytes=-$smallRequestEndBytes")
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 416")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads_finalBytes_prefixOffset(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val startBytes = 4500L
    val totalBytes = FIVE_MB
    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
        it.range("bytes=$startBytes-")
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(totalBytes - startBytes)
        assertThat(it.response().contentRange()).isEqualTo("bytes $startBytes-${totalBytes - 1}/$totalBytes")
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads_finalBytes_suffixOffset(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val endBytes = 500L
    val totalBytes = FIVE_MB
    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
        it.range("bytes=-$endBytes")
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(endBytes)
        assertThat(it.response().contentRange()).isEqualTo("bytes ${totalBytes - endBytes}-${totalBytes - 1}/$totalBytes")
      }
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  @S3VerifiedFailure(
    year = 2023,
    reason = "No KMS configuration for AWS test account",
  )
  fun testPutObject_withEncryption(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)

    val sseCustomerAlgorithm = "someCustomerAlgorithm"
    val sseCustomerKey = "someCustomerKey"
    val sseCustomerKeyMD5 = "someCustomerKeyMD5"
    val ssekmsEncryptionContext = "someEncryptionContext"
    s3Client
      .putObject(
        {
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
          it.ssekmsKeyId(TEST_ENC_KEY_ID)
          it.sseCustomerAlgorithm(sseCustomerAlgorithm)
          it.sseCustomerKey(sseCustomerKey)
          it.sseCustomerKeyMD5(sseCustomerKeyMD5)
          it.ssekmsEncryptionContext(ssekmsEncryptionContext)
          it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
        },
        RequestBody.fromFile(UPLOAD_FILE),
      ).also {
        assertThat(it.ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
        assertThat(it.sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
        assertThat(it.sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
        assertThat(it.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
      }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
      }.use {
        assertThat(it.response().ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
        assertThat(it.response().sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
        assertThat(it.response().sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
        assertThat(it.response().serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
      }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest(name = ParameterizedTest.INDEX_PLACEHOLDER + " uploadWithSigning={0}, uploadChunked={1}")
  @CsvSource(value = ["true, true", "true, false", "false, true", "false, false"])
  fun testPutGetObject_signingAndChunkedEncoding(
    uploadWithSigning: Boolean,
    uploadChunked: Boolean,
    testInfo: TestInfo,
  ) {
    val key = UPLOAD_FILE_NAME
    val bucketName = givenBucket(testInfo)

    val s3Client = createS3Client(chunkedEncodingEnabled = uploadChunked)

    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(UPLOAD_FILE),
    )

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3Client
      .getObject {
        it.bucket(bucketName)
        it.key(key)
      }.use {
        assertThat(it.response().contentLength()).isEqualTo(UPLOAD_FILE_LENGTH)
      }
  }

  private fun givenObjectV2WithRandomBytes(bucketName: String): String {
    val key = randomName
    s3Client.putObject(
      {
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromBytes(random5MBytes()),
    )
    return key
  }

  companion object {
    private const val NON_EXISTING_KEY = "NoSuchKey.json"
    private const val NO_SUCH_KEY = "The specified key does not exist."
    private const val NO_SUCH_BUCKET = "The specified bucket does not exist"
  }
}
