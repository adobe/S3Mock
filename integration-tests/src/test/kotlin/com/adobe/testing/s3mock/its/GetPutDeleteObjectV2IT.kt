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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.http.ContentDisposition
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.ChecksumMode
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.transfer.s3.S3TransferManager
import java.io.File
import java.io.FileInputStream
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.math.min

internal class GetPutDeleteObjectV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()
  private val s3ClientV2Http: S3Client = createS3ClientV2(serviceEndpointHttp)
  private val s3AsyncClientV2: S3AsyncClient = createS3AsyncClientV2()
  private val s3AsyncClientV2Http: S3AsyncClient = createS3AsyncClientV2(serviceEndpointHttp)
  private val s3CrtAsyncClientV2: S3AsyncClient = createS3CrtAsyncClientV2()
  private val s3CrtAsyncClientV2Http: S3AsyncClient = createS3CrtAsyncClientV2(serviceEndpointHttp)
  private val autoS3CrtAsyncClientV2: S3AsyncClient = createAutoS3CrtAsyncClientV2()
  private val autoS3CrtAsyncClientV2Http: S3AsyncClient = createAutoS3CrtAsyncClientV2(serviceEndpointHttp)
  private val transferManagerV2: S3TransferManager = createTransferManagerV2()

  @Test
  @S3VerifiedTodo
  fun testPutGetHeadDeleteObject(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    s3ClientV2.putObject({
      it.bucket(bucketName)
      it.key(key)
    },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }

    s3ClientV2.deleteObject {
      it.bucket(bucketName)
      it.key(key)
    }

    assertThatThrownBy {
      s3ClientV2.getObject {
        it.bucket(bucketName)
        it.key(key)
      }
    }
      .isInstanceOf(NoSuchKeyException::class.java)
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetHeadDeleteObjects(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)
    val keys = listOf("${key}-1", "${key}-2", "${key}-3")
    keys.forEach { key ->
      s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
      },
        RequestBody.fromFile(uploadFile)
      )
    }

    s3ClientV2.deleteObjects {
      it.bucket(bucketName)
      it.delete {
        it.objects(
          { it.key("${key}-1") },
          { it.key("${key}-2") },
          { it.key("${key}-3") },
        )
      }
    }

    keys.forEach { key ->
      assertThatThrownBy {
        s3ClientV2.getObject {
          it.bucket(bucketName)
          it.key(key)
        }
      }
        .isInstanceOf(NoSuchKeyException::class.java)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetHeadDeleteObjects_nonExistentKey(testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)
    givenObject(bucketName, key)

    s3ClientV2.deleteObjects {
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
  fun testPutHeadGetObject_keyNames_safe(key: String, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["storageClasses"])
  fun testPutObject_storageClass(storageClass: StorageClass, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    val key = UPLOAD_FILE_NAME

    val eTag = s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
        it.storageClass(storageClass)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(key)
    }.also {
      assertThat(it.eTag()).isEqualTo(eTag)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
      if (storageClass == StorageClass.STANDARD) {
        //storageClass STANDARD is never returned from S3 APIs...
        assertThat(it.response().storageClass()).isNull()
      } else {
        assertThat(it.response().storageClass()).isEqualTo(storageClass)
      }
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["testFileNames"])
  fun testPutObject_etagCreation_sync(testFileName: String, testInfo: TestInfo) {
    testEtagCreation(testFileName, s3ClientV2, testInfo)
    testEtagCreation(testFileName, s3ClientV2Http, testInfo)
  }

  private fun GetPutDeleteObjectV2IT.testEtagCreation(
    testFileName: String,
    s3Client: S3Client,
    testInfo: TestInfo
  ) {
    val uploadFile = File(testFileName)
    val expectedEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }
    val bucketName = givenBucket(testInfo)
    s3Client.putObject({
        it.bucket(bucketName)
        it.key(testFileName)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag().also {
      assertThat(it).isNotBlank
      assertThat(it).isEqualTo(expectedEtag)
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["testFileNames"])
  fun testPutObject_etagCreation_async(testFileName: String) {
    testEtagCreation(testFileName, s3AsyncClientV2)
    testEtagCreation(testFileName, s3AsyncClientV2Http)
    testEtagCreation(testFileName, s3CrtAsyncClientV2)
    testEtagCreation(testFileName, s3CrtAsyncClientV2Http)
    testEtagCreation(testFileName, autoS3CrtAsyncClientV2)
    testEtagCreation(testFileName, autoS3CrtAsyncClientV2Http)
  }

  private fun GetPutDeleteObjectV2IT.testEtagCreation(
    testFileName: String,
    s3Client: S3AsyncClient
  ) {
    val uploadFile = File(testFileName)
    val expectedEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }
    val bucketName = givenBucket(randomName)
    s3Client.putObject({
        it.bucket(bucketName)
        it.key(testFileName)
      },
      AsyncRequestBody.fromFile(uploadFile)
    ).join().eTag().also {
      assertThat(it).isNotBlank
      assertThat(it).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_getObjectAttributes(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), Algorithm.SHA1)
    val bucketName = givenBucket(testInfo)

    val eTag = s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.checksumAlgorithm(ChecksumAlgorithm.SHA1)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.getObjectAttributes {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.objectAttributes(
        ObjectAttributes.OBJECT_SIZE,
        ObjectAttributes.STORAGE_CLASS,
        ObjectAttributes.E_TAG,
        ObjectAttributes.CHECKSUM
      )
    }.also {
      //
      assertThat(it.eTag()).isEqualTo(eTag.trim('"'))
      //default storageClass is STANDARD, which is never returned from APIs
      assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
      assertThat(it.objectSize()).isEqualTo(File(UPLOAD_FILE_NAME).length())
      assertThat(it.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_objectMetadata(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    val eTag = s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.metadata(mapOf("key1" to "value1", "key2" to "value2"))
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.also {
      //
      assertThat(it.response().metadata()).containsAllEntriesOf(mapOf("key1" to "value1", "key2" to "value2"))
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_http(checksumAlgorithm: ChecksumAlgorithm) {
    if(checksumAlgorithm != ChecksumAlgorithm.SHA256) {
      //TODO: find out why the SHA256 checksum sent by the Java SDKv2 is wrong and this test is failing...
      testChecksumAlgorithm(SAMPLE_FILE, checksumAlgorithm, s3ClientV2Http)
      testChecksumAlgorithm(SAMPLE_FILE_LARGE, checksumAlgorithm, s3ClientV2Http)
      testChecksumAlgorithm(TEST_IMAGE, checksumAlgorithm, s3ClientV2Http)
      testChecksumAlgorithm(TEST_IMAGE_LARGE, checksumAlgorithm, s3ClientV2Http)
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_https(checksumAlgorithm: ChecksumAlgorithm) {
    testChecksumAlgorithm(SAMPLE_FILE, checksumAlgorithm, s3ClientV2)
    testChecksumAlgorithm(SAMPLE_FILE_LARGE, checksumAlgorithm, s3ClientV2)
    testChecksumAlgorithm(TEST_IMAGE, checksumAlgorithm, s3ClientV2)
    testChecksumAlgorithm(TEST_IMAGE_LARGE, checksumAlgorithm, s3ClientV2)
  }

  private fun GetPutDeleteObjectV2IT.testChecksumAlgorithm(
      testFileName: String,
      checksumAlgorithm: ChecksumAlgorithm,
      s3Client: S3Client,
  ) {
    val uploadFile = File(testFileName)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val bucketName = givenBucket(randomName)

    s3Client.putObject({
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumAlgorithm(checksumAlgorithm)
      },
      RequestBody.fromFile(uploadFile)
    ).also {
      val putChecksum = it.checksum(checksumAlgorithm)
      assertThat(putChecksum).isNotBlank
      assertThat(putChecksum).isEqualTo(expectedChecksum)
    }

    s3Client.getObject {
      it.bucket(bucketName)
      it.key(testFileName)
      it.checksumMode(ChecksumMode.ENABLED)
    }.use {
      val getChecksum = it.response().checksum(checksumAlgorithm)
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(testFileName)
      it.checksumMode(ChecksumMode.ENABLED)
    }.also {
      val headChecksum = it.checksum(checksumAlgorithm)
      assertThat(headChecksum).isNotBlank
      assertThat(headChecksum).isEqualTo(expectedChecksum)
    }
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_async_http(checksumAlgorithm: ChecksumAlgorithm) {
    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3AsyncClientV2Http)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3AsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3AsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3AsyncClientV2Http)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3CrtAsyncClientV2Http)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, autoS3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, autoS3CrtAsyncClientV2Http)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientV2Http)
  }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm_async_https(checksumAlgorithm: ChecksumAlgorithm) {
    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3AsyncClientV2)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3AsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3AsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3AsyncClientV2)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, s3CrtAsyncClientV2)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, s3CrtAsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, s3CrtAsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, s3CrtAsyncClientV2)

    testChecksumAlgorithm_async(SAMPLE_FILE, checksumAlgorithm, autoS3CrtAsyncClientV2)
    testChecksumAlgorithm_async(SAMPLE_FILE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE, checksumAlgorithm, autoS3CrtAsyncClientV2)
    testChecksumAlgorithm_async(TEST_IMAGE_LARGE, checksumAlgorithm, autoS3CrtAsyncClientV2)
  }

  private fun GetPutDeleteObjectV2IT.testChecksumAlgorithm_async(
      testFileName: String,
      checksumAlgorithm: ChecksumAlgorithm,
      s3Client: S3AsyncClient,
  ) {
    val uploadFile = File(testFileName)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val bucketName = givenBucket(randomName)

    s3Client.putObject({
        it.bucket(bucketName)
        it.key(testFileName)
        it.checksumAlgorithm(checksumAlgorithm)
      },
      AsyncRequestBody.fromFile(uploadFile)
    ).join().also {
      val putChecksum = it.checksum(checksumAlgorithm)
      assertThat(putChecksum).isNotBlank
      assertThat(putChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(testFileName)
      it.checksumMode(ChecksumMode.ENABLED)
    }.use {
      val getChecksum = it.response().checksum(checksumAlgorithm)
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(testFileName)
      it.checksumMode(ChecksumMode.ENABLED)
    }.also {
      val headChecksum = it.checksum(checksumAlgorithm)
      assertThat(headChecksum).isNotBlank
      assertThat(headChecksum).isEqualTo(expectedChecksum)
    }
  }

  private fun PutObjectRequest.Builder
    .checksum(checksum: String, checksumAlgorithm: ChecksumAlgorithm): PutObjectRequest.Builder =
    when (checksumAlgorithm) {
      ChecksumAlgorithm.SHA1 -> this.checksumSHA1(checksum)
      ChecksumAlgorithm.SHA256 -> this.checksumSHA256(checksum)
      ChecksumAlgorithm.CRC32 -> this.checksumCRC32(checksum)
      ChecksumAlgorithm.CRC32_C -> this.checksumCRC32C(checksum)
      //ChecksumAlgorithm.CRC64_NVME -> this.checksumCRC64NVME(checksum)
      else -> error("Unknown checksum algorithm")
    }

  @S3VerifiedSuccess(year = 2025)
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksum(checksumAlgorithm: ChecksumAlgorithm, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val bucketName = givenBucket(testInfo)

    s3ClientV2.putObject({
        it.checksum(expectedChecksum, checksumAlgorithm)
        it.bucket(bucketName).key(UPLOAD_FILE_NAME)
      },
      RequestBody.fromFile(uploadFile)
    ).also {
      val putChecksum = it.checksum(checksumAlgorithm)!!
      assertThat(putChecksum).isNotBlank
      assertThat(putChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.checksumMode(ChecksumMode.ENABLED)
    }.use {
      val getChecksum = it.response().checksum(checksumAlgorithm)
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.checksumMode(ChecksumMode.ENABLED)
    }.also {
      val headChecksum = it.checksum(checksumAlgorithm)
      assertThat(headChecksum).isNotBlank
      assertThat(headChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_wrongChecksum(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "wrongChecksum"
    val checksumAlgorithm = ChecksumAlgorithm.SHA1
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3ClientV2.putObject({
          it.checksum(expectedChecksum, checksumAlgorithm)
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Value for x-amz-checksum-sha1 header is invalid.")
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_wrongEncryptionKey(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    assertThatThrownBy {
      s3ClientV2.putObject({
          it.ssekmsKeyId(TEST_WRONG_KEY_ID)
          it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
          it.bucket(bucketName)
          it.key(UPLOAD_FILE_NAME)
        },
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 400")
      .hasMessageContaining("Key ID key-ID-WRONGWRONGWRONG does not exist!")
  }

  /**
   * Safe characters:
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutObject_safeCharacters(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    val key = "someKey${charsSafeKey()}"

    val eTag = s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(key)
    }.also {
      assertThat(it.eTag()).isEqualTo(eTag)
    }

    s3ClientV2.getObject {
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
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    val key = "someKey${charsSpecialKey()}"

    val eTag = s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).also {
      assertThat(it.eTag()).isEqualTo(eTag)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      assertThat(eTag).isEqualTo(it.response().eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetDeleteObject_twoBuckets(testInfo: TestInfo) {
    val bucket1 = givenBucket()
    val bucket2 = givenBucket()
    givenObject(bucket1, UPLOAD_FILE_NAME)
    givenObject(bucket2, UPLOAD_FILE_NAME)
    getObject(bucket1, UPLOAD_FILE_NAME)

    deleteObject(bucket1, UPLOAD_FILE_NAME)
    assertThatThrownBy {
      getObject(bucket1, UPLOAD_FILE_NAME)
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")

    getObject(bucket2, UPLOAD_FILE_NAME)
      .use {
        assertThat(getObject(bucket2, UPLOAD_FILE_NAME).response().eTag()).isEqualTo(it.response().eTag())
      }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testPutGetHeadObject_storeHeaders(testInfo: TestInfo) {
    val bucket = givenBucket()
    val uploadFile = File(UPLOAD_FILE_NAME)
    val contentDisposition = ContentDisposition.formData()
      .name("file")
      .filename("sampleFile.txt")
      .build()
      .toString()
    val expires = Instant.now()
    val encoding = "SomeEncoding"
    val contentLanguage = "SomeLanguage"
    val cacheControl = "SomeCacheControl"

    s3ClientV2.putObject({
        it.bucket(bucket)
        it.key(UPLOAD_FILE_NAME)
        it.contentDisposition(contentDisposition)
        it.contentEncoding(encoding)
        it.expires(expires)
        it.contentLanguage(contentLanguage)
        it.cacheControl(cacheControl)
      },
      RequestBody.fromFile(uploadFile)
    )

    getObject(bucket, UPLOAD_FILE_NAME).also {
      assertThat(it.response().contentDisposition()).isEqualTo(contentDisposition)
      assertThat(it.response().contentEncoding()).isEqualTo(encoding)
      // time in second precision, see
      // https://www.rfc-editor.org/rfc/rfc7234#section-5.3
      // https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
      assertThat(it.response().expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
      assertThat(it.response().contentLanguage()).isEqualTo(contentLanguage)
      assertThat(it.response().cacheControl()).isEqualTo(cacheControl)
    }


    s3ClientV2.headObject {
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
  fun testGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val matchingEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }

    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(matchingEtag)
    }

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(matchingEtag)
    }.use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_successWithSameLength(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val matchingEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }

    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(matchingEtag)
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_successWithMatchingWildcardEtag(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val matchingEtag = "\"*\""

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(matchingEtag)
    }.use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testHeadObject_successWithNonMatchEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }

    val nonMatchingEtag = "\"$randomName\""

    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    s3ClientV2.headObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifNoneMatch(nonMatchingEtag)
    }.also {
      assertThat(it.eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testHeadObject_failureWithNonMatchWildcardEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedEtag = FileInputStream(uploadFile).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }

    val nonMatchingEtag = "\"*\""

    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    assertThatThrownBy {
      s3ClientV2.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifNoneMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testHeadObject_failureWithMatchEtag(testInfo: TestInfo) {
    val expectedEtag = FileInputStream(File(UPLOAD_FILE_NAME)).let {
      "\"${DigestUtil.hexDigest(it)}\""
    }

    val nonMatchingEtag = "\"$randomName\""

    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    assertThatThrownBy {
      s3ClientV2.headObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifMatch(nonMatchingEtag)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_successWithMatchingIfModified(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifModifiedSince(now)
    }.use {
      assertThat(it.response().eTag()).isNotNull()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_failureWithNonMatchingIfModified(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    TimeUnit.SECONDS.sleep(10L)
    val now = Instant.now()

    assertThatThrownBy {
      s3ClientV2.getObject {
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ifModifiedSince(now)
      }
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_successWithMatchingIfUnmodified(testInfo: TestInfo) {
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val now = Instant.now().plusSeconds(60)

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifUnmodifiedSince(now)
    }.use {
      assertThat(it.response().eTag()).isNotNull()
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_failureWithNonMatchingIfUnmodified(testInfo: TestInfo) {
    val now = Instant.now().minusSeconds(60)
    val (bucketName, _) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)

    assertThatThrownBy {
      s3ClientV2.getObject {
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
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResponse) = givenBucketAndObject(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val smallRequestStartBytes = 1L
    val smallRequestEndBytes = 2L

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.ifMatch(eTag)
      it.range("bytes=$smallRequestStartBytes-$smallRequestEndBytes")
    }.also {
      assertThat(it.response().contentLength()).isEqualTo(smallRequestEndBytes)
      assertThat(it.response().contentRange())
        .isEqualTo("bytes $smallRequestStartBytes-$smallRequestEndBytes/${uploadFile.length()}")
    }

    val largeRequestStartBytes = 0L
    val largeRequestEndBytes = 1000L

    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
      it.range("bytes=$largeRequestStartBytes-$largeRequestEndBytes")
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(min(uploadFile.length(), largeRequestEndBytes + 1))
      assertThat(it.response().contentRange())
        .isEqualTo(
          "bytes $largeRequestStartBytes-${min(uploadFile.length() - 1, largeRequestEndBytes)}/${uploadFile.length()}"
        )
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads_finalBytes_prefixOffset(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val startBytes = 4500L
    val totalBytes = _5MB.toInt()
    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
      it.range("bytes=$startBytes-")
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(totalBytes - startBytes)
      assertThat(it.response().contentRange()).isEqualTo("bytes $startBytes-${totalBytes-1}/$totalBytes")
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2025)
  fun testGetObject_rangeDownloads_finalBytes_suffixOffset(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val endBytes = 500L
    val totalBytes = _5MB.toInt()
    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(key)
      it.range("bytes=-$endBytes")
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(endBytes)
      assertThat(it.response().contentRange()).isEqualTo("bytes ${totalBytes-endBytes}-${totalBytes-1}/$totalBytes")
    }
  }

  /**
   * Tests if Object can be uploaded with KMS and Metadata can be retrieved.
   */
  @Test
  @S3VerifiedFailure(year = 2023,
    reason = "No KMS configuration for AWS test account")
  fun testPutObject_withEncryption(testInfo: TestInfo) {
    val bucketName = givenBucket(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)

    val sseCustomerAlgorithm = "someCustomerAlgorithm"
    val sseCustomerKey = "someCustomerKey"
    val sseCustomerKeyMD5 = "someCustomerKeyMD5"
    val ssekmsEncryptionContext = "someEncryptionContext"
    s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(UPLOAD_FILE_NAME)
        it.ssekmsKeyId(TEST_ENC_KEY_ID)
        it.sseCustomerAlgorithm(sseCustomerAlgorithm)
        it.sseCustomerKey(sseCustomerKey)
        it.sseCustomerKeyMD5(sseCustomerKeyMD5)
        it.ssekmsEncryptionContext(ssekmsEncryptionContext)
        it.serverSideEncryption(ServerSideEncryption.AWS_KMS)
      },
      RequestBody.fromFile(uploadFile)
    ).also {
      assertThat(it.ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
      assertThat(it.sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
      assertThat(it.sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
      assertThat(it.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
    }


    s3ClientV2.getObject {
      it.bucket(bucketName)
      it.key(UPLOAD_FILE_NAME)
    }.use {
      assertThat(it.response().ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
      assertThat(it.response().sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
      assertThat(it.response().sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
      assertThat(it.response().serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
    }
  }

  @ParameterizedTest(name = ParameterizedTest.INDEX_PLACEHOLDER + " uploadWithSigning={0}, uploadChunked={1}")
  @CsvSource(value = ["true, true", "true, false", "false, true", "false, false"])
  fun testPutGetObject_signingAndChunkedEncoding(uploadWithSigning: Boolean, uploadChunked: Boolean, testInfo: TestInfo) {
    val key = UPLOAD_FILE_NAME
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucket(testInfo)

    val s3Client = createS3ClientV2(chunkedEncodingEnabled = uploadChunked)

    s3Client.putObject({
      it.bucket(bucketName)
      it.key(key)
    },
      RequestBody.fromFile(uploadFile)
    )

    s3Client.headObject {
      it.bucket(bucketName)
      it.key(key)
    }

    s3Client.getObject {
      it.bucket(bucketName)
      it.key(key)
    }.use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  private fun givenObjectV2WithRandomBytes(bucketName: String): String {
    val key = randomName
    s3ClientV2.putObject({
        it.bucket(bucketName)
        it.key(key)
      },
      RequestBody.fromBytes(random5MBytes())
    )
    return key
  }
}
