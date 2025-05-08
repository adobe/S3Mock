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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.ChecksumTestUtil
import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.Delete
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.isNull
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.http.HttpHeaders
import org.springframework.util.MultiValueMapAdapter
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockBean(classes = [BucketService::class, MultipartService::class, MultipartStore::class])
internal class ObjectServiceTest : ServiceTestBase() {
  @Autowired
  private lateinit var iut: ObjectService

  @Test
  fun testDeleteObjects() {
    val bucketName = "bucket"
    val key = "key"
    val key2 = "key2"
    givenBucketWithContents(
      bucketName, "", listOf(
        givenS3Object(key),
        givenS3Object(key2)
      )
    )
    val delete = Delete(
      listOf(
        givenS3ObjectIdentifier(key),
        givenS3ObjectIdentifier(key2)
      ), false
    )

    whenever(objectStore.deleteObject(any(BucketMetadata::class.java), any(UUID::class.java), isNull()))
      .thenReturn(true)
    whenever(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true)
    whenever(bucketStore.removeFromBucket(key2, bucketName)).thenReturn(true)
    val deleted = iut.deleteObjects(bucketName, delete)
    assertThat(deleted.deletedObjects).hasSize(2)
  }

  private fun givenS3ObjectIdentifier(key: String?): S3ObjectIdentifier {
    return S3ObjectIdentifier(key, null, null, null, null)
  }

  @Test
  fun testDeleteObject() {
    val bucketName = "bucket"
    val key = "key"
    givenBucketWithContents(bucketName, "", listOf(givenS3Object(key)))
    whenever(objectStore.deleteObject(any(BucketMetadata::class.java), any(UUID::class.java), isNull()))
      .thenReturn(true)
    whenever(bucketStore.removeFromBucket(key, bucketName)).thenReturn(true)
    val deleted = iut.deleteObject(bucketName, key, null)
    assertThat(deleted).isTrue()
  }

  @Test
  fun testVerifyRetention_success() {
    val retention = Retention(Mode.COMPLIANCE, Instant.now().plus(1, ChronoUnit.MINUTES))

    iut.verifyRetention(retention)
  }

  @Test
  fun testVerifyRetention_failure() {
    val retention = Retention(Mode.COMPLIANCE, Instant.now().minus(1, ChronoUnit.MINUTES))
    assertThatThrownBy { iut.verifyRetention(retention) }
      .isEqualTo(S3Exception.INVALID_REQUEST_RETAIN_DATE)
  }

  @Test
  @Throws(IOException::class)
  fun testVerifyMd5_success() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val md5 = DigestUtil.base64Digest(Files.newInputStream(path))
    iut.verifyMd5(path, md5)
  }

  @Test
  fun testVerifyMd5_failure() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val md5 = "wrong-md5"
    assertThatThrownBy { iut.verifyMd5(path, md5) }.isEqualTo(S3Exception.BAD_REQUEST_MD5)
  }

  @Test
  @Throws(IOException::class)
  fun testVerifyMd5Void_success() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val md5 = DigestUtil.base64Digest(Files.newInputStream(path))
    iut.verifyMd5(Files.newInputStream(path), md5)
  }

  @Test
  fun testVerifyMd5Void_failure() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val md5 = "wrong-md5"
    assertThatThrownBy { iut.verifyMd5(Files.newInputStream(path), md5) }.isEqualTo(
      S3Exception.BAD_REQUEST_MD5
    )
  }

  @Test
  fun testVerifyObjectMatching_matchSuccess() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val etag = "\"etag\""

    iut.verifyObjectMatching(
      listOf(etag),
      null,
      null,
      null,
      s3ObjectMetadata
    )
  }

  @Test
  fun testVerifyObjectMatching_matchWildcard() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val etag = "\"nonematch\""

    iut.verifyObjectMatching(listOf(etag, ObjectService.WILDCARD_ETAG), null, null, null, s3ObjectMetadata)
  }

  @Test
  fun testVerifyObjectMatching_matchFailure() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val etag = "\"nonematch\""

    assertThatThrownBy { iut.verifyObjectMatching(listOf(etag), null, null, null, s3ObjectMetadata) }
      .isEqualTo(S3Exception.PRECONDITION_FAILED)
  }

  @Test
  fun testVerifyObjectMatching_ifModifiedFailure() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val now = Instant.now().plusSeconds(10)

    assertThatThrownBy { iut.verifyObjectMatching(null, null, listOf(now), null, s3ObjectMetadata) }
      .isEqualTo(S3Exception.NOT_MODIFIED)
  }

  @Test
  fun testVerifyObjectMatching_ifModifiedSuccess() {
    val key = "key"
    val now = Instant.now().minusSeconds(10)
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)

    iut.verifyObjectMatching(null, null, listOf(now), null, s3ObjectMetadata)
  }

  @Test
  fun testVerifyObjectMatching_ifUnmodifiedFailure() {
    val key = "key"
    val now = Instant.now().minusSeconds(10)
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)

    assertThatThrownBy { iut.verifyObjectMatching(null, null, null, listOf(now), s3ObjectMetadata) }
      .isEqualTo(S3Exception.PRECONDITION_FAILED)
  }

  @Test
  fun testVerifyObjectMatching_ifUnmodifiedSuccess() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val now = Instant.now().plusSeconds(10)

    iut.verifyObjectMatching(null, null, null, listOf(now), s3ObjectMetadata)
  }

  @Test
  fun testVerifyObjectMatching_noneMatchSuccess() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val etag = "\"nonematch\""

    iut.verifyObjectMatching(null, listOf(etag), null, null, s3ObjectMetadata)
  }

  @Test
  fun testVerifyObjectMatching_noneMatchWildcard() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)

    assertThatThrownBy {
      iut.verifyObjectMatching(
        null,
        listOf(ObjectService.WILDCARD_ETAG),
        null, null,
        s3ObjectMetadata
      )
    }
      .isEqualTo(S3Exception.NOT_MODIFIED)
  }

  @Test
  fun testVerifyObjectMatching_noneMatchFailure() {
    val key = "key"
    val s3ObjectMetadata = s3ObjectMetadata(UUID.randomUUID(), key)
    val etag = "\"etag\""

    assertThatThrownBy {
      iut.verifyObjectMatching(
        null,
        listOf(etag),
        null,
        null,
        s3ObjectMetadata
      )
    }.isEqualTo(S3Exception.NOT_MODIFIED)
  }

  @Test
  fun testVerifyObjectLockConfiguration_failure() {
    val bucketName = "bucket"
    val prefix = ""
    val key = "key"
    givenBucketWithContents(bucketName, prefix, listOf(givenS3Object(key)))
    assertThatThrownBy { iut.verifyObjectLockConfiguration(bucketName, key, null) }
      .isEqualTo(S3Exception.NOT_FOUND_OBJECT_LOCK)
  }

  @Test
  fun testVerifyObjectExists_success() {
    val bucketName = "bucket"
    val prefix = ""
    val key = "key"
    givenBucketWithContents(bucketName, prefix, listOf(givenS3Object(key)))
    val s3ObjectMetadata = iut.verifyObjectExists(bucketName, key, null)
    assertThat(s3ObjectMetadata.key).isEqualTo(key)
  }

  @Test
  fun testVerifyObjectExists_failure() {
    val bucketName = "bucket"
    val key = "key"
    givenBucket(bucketName)
    assertThatThrownBy { iut.verifyObjectExists(bucketName, key, null) }
      .isEqualTo(S3Exception.NO_SUCH_KEY)
  }

  @Test
  @Throws(IOException::class)
  fun test_toTempFile() {
    val file = File("src/test/resources/sampleFile_large.txt")
    val tempFile = toTempFile(file.toPath(), DefaultChecksumAlgorithm.SHA256)
    val tempFileAndChecksum = iut.toTempFile(
      Files.newInputStream(tempFile),
      HttpHeaders(
        MultiValueMapAdapter(
          mapOf(
            AwsHttpHeaders.X_AMZ_SDK_CHECKSUM_ALGORITHM to listOf(ChecksumAlgorithm.SHA256.toString()),
            HttpHeaders.CONTENT_ENCODING to listOf(AwsHttpHeaders.AWS_CHUNKED),
            AwsHttpHeaders.X_AMZ_TRAILER to listOf(AwsHttpHeaders.X_AMZ_CHECKSUM_SHA256)
          )
        )
      )
    )
    assertThat(tempFileAndChecksum.left.fileName.toString()).contains("toTempFile")
    assertThat(tempFileAndChecksum.right).contains("Y8S4/uAGut7vjdFZQjLKZ7P28V9EPWb4BIoeniuM0mY=")
  }

  @Throws(IOException::class)
  private fun toTempFile(path: Path, algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm): Path {
    val (inputStream, _) = ChecksumTestUtil.prepareInputStream(path.toFile(), false, algorithm)
    val tempFile = Files.createTempFile("temp", "")
    inputStream.use { chunkedEncodingInputStream ->
        Files.newOutputStream(tempFile).use { outputStream ->
          chunkedEncodingInputStream.transferTo(outputStream)
        }
      }
    return tempFile
  }

  companion object {
    private const val TEST_FILE_PATH = "src/test/resources/sampleFile.txt"
  }
}
