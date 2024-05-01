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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.http.ContentDisposition
import software.amazon.awssdk.core.checksums.Algorithm
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.transfer.s3.S3TransferManager
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.math.min

internal class GetPutDeleteObjectV2IT : S3TestBase() {

  private val s3ClientV2: S3Client = createS3ClientV2()
  private val transferManagerV2: S3TransferManager = createTransferManagerV2()

  /**
   * Test safe characters in object keys
   *
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @ParameterizedTest
  @MethodSource(value = ["charsSafe", "charsSpecial", "charsToAvoid"])
  fun testPutHeadGetObject_keyNames_safe(key: String, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).also {
      assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
    }

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @S3VerifiedTodo
  @ParameterizedTest
  @MethodSource(value = ["storageClasses"])
  fun testPutObject_storageClass(storageClass: StorageClass, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    val key = UPLOAD_FILE_NAME

    val eTag = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .storageClass(storageClass)
        .build(),
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

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutObject_etagCreation(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val (_, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    putObjectResponse.eTag().also {
      assertThat(it).isNotBlank
      assertThat(it).isEqualTo(expectedEtag)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_getObjectAttributes(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), Algorithm.SHA1)
    val bucketName = givenBucketV2(testInfo)

    val eTag = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.SHA1)
        .build(),
      RequestBody.fromFile(uploadFile)
    ).eTag()

    s3ClientV2.getObjectAttributes(
      GetObjectAttributesRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .objectAttributes(
          ObjectAttributes.OBJECT_SIZE,
          ObjectAttributes.STORAGE_CLASS,
          ObjectAttributes.E_TAG,
          ObjectAttributes.CHECKSUM)
        .build()
    ).also {
      assertThat(it.eTag()).isEqualTo(eTag)
      assertThat(it.storageClass()).isEqualTo(StorageClass.STANDARD)
      assertThat(it.objectSize()).isEqualTo(File(UPLOAD_FILE_NAME).length())
      assertThat(it.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
    }
  }

  @S3VerifiedTodo
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksumAlgorithm(checksumAlgorithm: ChecksumAlgorithm, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(checksumAlgorithm)
        .build(),
      RequestBody.fromFile(uploadFile)
    ).also {
      val putChecksum = it.checksum(checksumAlgorithm)
      assertThat(putChecksum).isNotBlank
      assertThat(putChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksum(checksumAlgorithm)
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).also {
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
      else -> error("Unknown checksum algorithm")
    }

  @S3VerifiedTodo
  @ParameterizedTest
  @MethodSource(value = ["checksumAlgorithms"])
  fun testPutObject_checksum(checksumAlgorithm: ChecksumAlgorithm, testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = DigestUtil.checksumFor(uploadFile.toPath(), checksumAlgorithm.toAlgorithm())
    val bucketName = givenBucketV2(testInfo)

    s3ClientV2.putObject(
      PutObjectRequest
        .builder()
        .checksum(expectedChecksum, checksumAlgorithm)
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .build(),
      RequestBody.fromFile(uploadFile)
    ).also {
      val putChecksum = it.checksum(checksumAlgorithm)
      assertThat(putChecksum).isNotBlank
      assertThat(putChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksum(checksumAlgorithm)
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }

    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).also {
      val headChecksum = it.checksum(checksumAlgorithm)
      assertThat(headChecksum).isNotBlank
      assertThat(headChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_wrongChecksum(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "wrongChecksum"
    val checksumAlgorithm = ChecksumAlgorithm.SHA1
    val bucketName = givenBucketV2(testInfo)

    assertThatThrownBy {
      s3ClientV2.putObject(
        PutObjectRequest
          .builder()
          .checksum(expectedChecksum, checksumAlgorithm)
          .bucket(bucketName).key(UPLOAD_FILE_NAME)
          .build(),
        RequestBody.fromFile(uploadFile)
      )
    }
      .isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("The Content-MD5 or checksum value that you specified did not match what the server received.")
  }

  /**
   * Safe characters:
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedTodo
  fun testPutObject_safeCharacters(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    val key = "someKey${charsSafeKey()}"

    val eTag = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
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

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(eTag).isEqualTo(it.response().eTag())
    }
  }

  /**
   * Characters needing special handling:
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
   */
  @Test
  @S3VerifiedTodo
  fun testPutObject_specialHandlingCharacters(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    val key = "someKey${charsSpecialKey()}"

    val eTag = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
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

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(eTag).isEqualTo(it.response().eTag())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutGetDeleteObject_twoBuckets(testInfo: TestInfo) {
    val bucket1 = givenRandomBucketV2()
    val bucket2 = givenRandomBucketV2()
    givenObjectV2(bucket1, UPLOAD_FILE_NAME)
    givenObjectV2(bucket2, UPLOAD_FILE_NAME)
    getObjectV2(bucket1, UPLOAD_FILE_NAME)

    deleteObjectV2(bucket1, UPLOAD_FILE_NAME)
    assertThatThrownBy {
      getObjectV2(bucket1, UPLOAD_FILE_NAME)
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 404")

    getObjectV2(bucket2, UPLOAD_FILE_NAME)
      .use {
        assertThat(getObjectV2(bucket2, UPLOAD_FILE_NAME).response().eTag()).isEqualTo(it.response().eTag())
      }
  }

  @Test
  @S3VerifiedTodo
  fun testPutGetHeadObject_storeHeaders(testInfo: TestInfo) {
    val bucket = givenRandomBucketV2()
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

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucket)
        .key(UPLOAD_FILE_NAME)
        .contentDisposition(contentDisposition)
        .contentEncoding(encoding)
        .expires(expires)
        .contentLanguage(contentLanguage)
        .cacheControl(cacheControl)
        .build(),
      RequestBody.fromFile(uploadFile))

    getObjectV2(bucket, UPLOAD_FILE_NAME).also {
      assertThat(it.response().contentDisposition()).isEqualTo(contentDisposition)
      assertThat(it.response().contentEncoding()).isEqualTo(encoding)
      // time in second precision, see
      // https://www.rfc-editor.org/rfc/rfc7234#section-5.3
      // https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
      assertThat(it.response().expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
      assertThat(it.response().contentLanguage()).isEqualTo(contentLanguage)
      assertThat(it.response().cacheControl()).isEqualTo(cacheControl)
    }


    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucket)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).also {
      assertThat(it.contentDisposition()).isEqualTo(contentDisposition)
      assertThat(it.contentEncoding()).isEqualTo(encoding)
      assertThat(it.expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
      assertThat(it.contentLanguage()).isEqualTo(contentLanguage)
      assertThat(it.cacheControl()).isEqualTo(cacheControl)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val matchingEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(matchingEtag)
    }

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifMatch(matchingEtag)
        .build()
    ).use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_successWithSameLength(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val matchingEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val (bucketName, _) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifMatch(matchingEtag)
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(uploadFile.length())
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetObject_successWithMatchingWildcardEtag(testInfo: TestInfo) {
    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val matchingEtag = "\"*\""

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifMatch(matchingEtag)
        .build()
    ).use {
      assertThat(it.response().eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testHeadObject_successWithNonMatchEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val nonMatchingEtag = "\"$randomName\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifNoneMatch(nonMatchingEtag)
        .build()
    ).also {
      assertThat(it.eTag()).isEqualTo(eTag)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testHeadObject_failureWithNonMatchWildcardEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val nonMatchingEtag = "\"*\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    assertThatThrownBy {
      s3ClientV2.headObject(
        HeadObjectRequest.builder()
          .bucket(bucketName)
          .key(UPLOAD_FILE_NAME)
          .ifNoneMatch(nonMatchingEtag)
          .build()
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 304")
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testHeadObject_failureWithMatchEtag(testInfo: TestInfo) {
    val expectedEtag = FileInputStream(File(UPLOAD_FILE_NAME))
      .let {"\"${DigestUtil.hexDigest(it)}\""}

    val nonMatchingEtag = "\"$randomName\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    putObjectResponse.eTag().also {
      assertThat(it).isEqualTo(expectedEtag)
    }

    assertThatThrownBy {
      s3ClientV2.headObject(
        HeadObjectRequest.builder()
          .bucket(bucketName)
          .key(UPLOAD_FILE_NAME)
          .ifMatch(nonMatchingEtag)
          .build()
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("Service: S3, Status Code: 412")
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_rangeDownloads(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val smallRequestStartBytes = 1L
    val smallRequestEndBytes = 2L

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifMatch(eTag)
        .range("bytes=$smallRequestStartBytes-$smallRequestEndBytes")
        .build()
    ).also {
      assertThat(it.response().contentLength()).isEqualTo(smallRequestEndBytes)
      assertThat(it.response().contentRange())
        .isEqualTo("bytes $smallRequestStartBytes-$smallRequestEndBytes/${uploadFile.length()}")
    }

    val largeRequestStartBytes = 0L
    val largeRequestEndBytes = 1000L

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .range("bytes=$largeRequestStartBytes-$largeRequestEndBytes")
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(min(uploadFile.length(), largeRequestEndBytes + 1))
      assertThat(it.response().contentRange())
        .isEqualTo(
          "bytes $largeRequestStartBytes-${min(uploadFile.length() - 1, largeRequestEndBytes)}/${uploadFile.length()}"
        )
    }
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_rangeDownloads_finalBytes_prefixOffset(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val startBytes = 4500L
    val totalBytes = _5MB.toInt()
    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .range("bytes=$startBytes-")
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(totalBytes - startBytes)
      assertThat(it.response().contentRange()).isEqualTo("bytes $startBytes-${totalBytes-1}/$totalBytes")
    }
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_rangeDownloads_finalBytes_suffixOffset(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)
    val endBytes = 500L
    val totalBytes = _5MB.toInt()
    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .range("bytes=-$endBytes")
        .build()
    ).use {
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
    val bucketName = givenBucketV2(testInfo)
    val uploadFile = File(UPLOAD_FILE_NAME)

    val sseCustomerAlgorithm = "someCustomerAlgorithm"
    val sseCustomerKey = "someCustomerKey"
    val sseCustomerKeyMD5 = "someCustomerKeyMD5"
    val ssekmsEncryptionContext = "someEncryptionContext"
    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ssekmsKeyId(TEST_ENC_KEY_ID)
        .sseCustomerAlgorithm(sseCustomerAlgorithm)
        .sseCustomerKey(sseCustomerKey)
        .sseCustomerKeyMD5(sseCustomerKeyMD5)
        .ssekmsEncryptionContext(ssekmsEncryptionContext)
        .serverSideEncryption(ServerSideEncryption.AWS_KMS)
        .build(),
      RequestBody.fromFile(uploadFile)
    ).also {
      assertThat(it.ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
      assertThat(it.sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
      assertThat(it.sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
      assertThat(it.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
    }


    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      assertThat(it.response().ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
      assertThat(it.response().sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
      assertThat(it.response().sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
      assertThat(it.response().serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)
    }
  }

  private fun givenObjectV2WithRandomBytes(bucketName: String): String {
    val key = randomName
    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
      RequestBody.fromBytes(random5MBytes())
    )
    return key
  }
}
