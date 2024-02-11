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
import org.springframework.http.ContentDisposition
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ObjectAttributes
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import software.amazon.awssdk.services.s3.model.StorageClass
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class GetPutDeleteObjectV2IT : S3TestBase() {

  @Test
  @S3VerifiedTodo
  fun testPutObject_storageClass(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val bucketName = givenBucketV2(testInfo)

    val key = UPLOAD_FILE_NAME

    s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .storageClass(StorageClass.DEEP_ARCHIVE)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val headObjectResponse = s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    )
    assertThat(headObjectResponse.storageClass()).isEqualTo(StorageClass.DEEP_ARCHIVE)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(it.response().storageClass()).isEqualTo(StorageClass.DEEP_ARCHIVE)
    }
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testPutObject_etagCreation(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val (_, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    assertThat(eTag).isNotBlank
    assertThat(eTag).isEqualTo(expectedEtag)
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_getObjectAttributes(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "+AXXQmKfnxMv0B57SJutbNpZBww="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.SHA1)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val eTag = putObjectResponse.eTag()

    val objectAttributes = s3ClientV2.getObjectAttributes(
      GetObjectAttributesRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .objectAttributes(
          ObjectAttributes.OBJECT_SIZE,
          ObjectAttributes.STORAGE_CLASS,
          ObjectAttributes.E_TAG,
          ObjectAttributes.CHECKSUM)
        .build()
    )

    assertThat(objectAttributes.eTag()).isEqualTo(eTag)
    assertThat(objectAttributes.storageClass()).isEqualTo(StorageClass.STANDARD)
    assertThat(objectAttributes.objectSize()).isEqualTo(File(UPLOAD_FILE_NAME).length())
    assertThat(objectAttributes.checksum().checksumSHA1()).isEqualTo(expectedChecksum)
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checksumAlgorithm_sha1(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "+AXXQmKfnxMv0B57SJutbNpZBww="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.SHA1)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumSHA1()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    val getObjectResponse = s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    )
    val getChecksum = getObjectResponse.response().checksumSHA1()
    assertThat(getChecksum).isNotBlank
    assertThat(getChecksum).isEqualTo(expectedChecksum)

    val headObjectResponse = s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    )
    val headChecksum = headObjectResponse.checksumSHA1()
    assertThat(headChecksum).isNotBlank
    assertThat(headChecksum).isEqualTo(expectedChecksum)

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

    val key = "someKey!-_.*'()" //safe characters as per S3 API

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(putObjectResponse.eTag()).isEqualTo(it.response().eTag())
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

    val key = "someKey&$@=;/:+ ,?" //safe characters as per S3 API

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()
    ).use {
      assertThat(putObjectResponse.eTag()).isEqualTo(it.response().eTag())
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checkSum_sha1(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "+AXXQmKfnxMv0B57SJutbNpZBww="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumSHA1(expectedChecksum)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumSHA1()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumSHA1()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checksumAlgorithm_sha256(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumSHA256()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumSHA256()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checkSum_sha256(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "1VcEifAruhjVvjzul4sC0B1EmlUdzqvsp6BP0KSVdTE="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumSHA256(expectedChecksum)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumSHA256()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumSHA256()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checksumAlgorithm_crc32(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "I6zdvg=="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.CRC32)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumCRC32()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumCRC32()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checkSum_crc32(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "I6zdvg=="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName).key(UPLOAD_FILE_NAME)
        .checksumCRC32(expectedChecksum)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumCRC32()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumCRC32()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checksumAlgorithm_crc32c(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "/Ho1Kg=="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumCRC32C()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumCRC32C()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
    }
  }

  @Test
  @S3VerifiedTodo
  fun testPutObject_checkSum_crc32c(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val expectedChecksum = "/Ho1Kg=="
    val bucketName = givenBucketV2(testInfo)

    val putObjectResponse = s3ClientV2.putObject(
      PutObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .checksumCRC32C(expectedChecksum)
        .build(),
      RequestBody.fromFile(uploadFile)
    )

    val putChecksum = putObjectResponse.checksumCRC32C()
        assertThat(putChecksum).isNotBlank
        assertThat(putChecksum).isEqualTo(expectedChecksum)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .build()
    ).use {
      val getChecksum = it.response().checksumCRC32C()
      assertThat(getChecksum).isNotBlank
      assertThat(getChecksum).isEqualTo(expectedChecksum)
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

    val getObjectResponseResponse = getObjectV2(bucket, UPLOAD_FILE_NAME)

    assertThat(getObjectResponseResponse.response().contentDisposition())
      .isEqualTo(contentDisposition)
    assertThat(getObjectResponseResponse.response().contentEncoding())
      .isEqualTo(encoding)
    // time in second precision, see
    // https://www.rfc-editor.org/rfc/rfc7234#section-5.3
    // https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
    assertThat(getObjectResponseResponse.response().expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
    assertThat(getObjectResponseResponse.response().contentLanguage()).isEqualTo(contentLanguage)
    assertThat(getObjectResponseResponse.response().cacheControl()).isEqualTo(cacheControl)

    val headObjectResponse = s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucket)
        .key(UPLOAD_FILE_NAME)
        .build()
    )
    assertThat(headObjectResponse.contentDisposition()).isEqualTo(contentDisposition)
    assertThat(headObjectResponse.contentEncoding()).isEqualTo(encoding)
    assertThat(headObjectResponse.expires()).isEqualTo(expires.truncatedTo(ChronoUnit.SECONDS))
    assertThat(headObjectResponse.contentLanguage()).isEqualTo(contentLanguage)
    assertThat(headObjectResponse.cacheControl()).isEqualTo(cacheControl)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testGetObject_successWithMatchingEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val matchingEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    assertThat(eTag).isEqualTo(matchingEtag)
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
    val eTag = putObjectResponse.eTag()
    assertThat(eTag).isEqualTo(expectedEtag)

    val headObjectResponse = s3ClientV2.headObject(
      HeadObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifNoneMatch(nonMatchingEtag)
        .build()
    )
    assertThat(headObjectResponse.eTag()).isEqualTo(eTag)
  }

  @Test
  @S3VerifiedSuccess(year = 2022)
  fun testHeadObject_failureWithNonMatchWildcardEtag(testInfo: TestInfo) {
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val nonMatchingEtag = "\"*\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    assertThat(eTag).isEqualTo(expectedEtag)

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
    val uploadFile = File(UPLOAD_FILE_NAME)
    val uploadFileIs: InputStream = FileInputStream(uploadFile)
    val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

    val nonMatchingEtag = "\"$randomName\""

    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    assertThat(eTag).isEqualTo(expectedEtag)

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
    val (bucketName, putObjectResponse) = givenBucketAndObjectV2(testInfo, UPLOAD_FILE_NAME)
    val eTag = putObjectResponse.eTag()
    val smallObject = s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .ifMatch(eTag)
        .range("bytes=1-2")
        .build()
    )
    assertThat(smallObject.response().contentLength()).isEqualTo(2L)
    assertThat(smallObject.response().contentRange()).isEqualTo("bytes 1-2/36")

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(UPLOAD_FILE_NAME)
        .range("bytes=0-1000")
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(36L)
      assertThat(it.response().contentRange()).isEqualTo("bytes 0-35/36")
    }
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_rangeDownloads_finalBytes_prefixOffset(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .range("bytes=4500-")
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(5238380L)
      assertThat(it.response().contentRange()).isEqualTo("bytes 4500-5242879/5242880")
    }
  }

  @Test
  @S3VerifiedTodo
  fun testGetObject_rangeDownloads_finalBytes_suffixOffset(testInfo: TestInfo) {
    val bucketName = givenBucketV2(testInfo)
    val key = givenObjectV2WithRandomBytes(bucketName)

    s3ClientV2.getObject(
      GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .range("bytes=-500")
        .build()
    ).use {
      assertThat(it.response().contentLength()).isEqualTo(500L)
      assertThat(it.response().contentRange()).isEqualTo("bytes 5242380-5242879/5242880")
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
    val putObject = s3ClientV2.putObject(
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
    )

    assertThat(putObject.ssekmsKeyId()).isEqualTo(TEST_ENC_KEY_ID)
    assertThat(putObject.sseCustomerAlgorithm()).isEqualTo(sseCustomerAlgorithm)
    assertThat(putObject.sseCustomerKeyMD5()).isEqualTo(sseCustomerKeyMD5)
    assertThat(putObject.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS)

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
