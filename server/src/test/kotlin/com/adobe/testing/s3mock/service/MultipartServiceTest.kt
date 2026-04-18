/*
 *  Copyright 2017-2026 Adobe.
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

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.Initiator
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.store.MultipartUploadInfo
import com.adobe.testing.s3mock.store.ObjectStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Path
import java.util.Date
import java.util.UUID

@SpringBootTest(classes = [ServiceConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@MockitoBean(types = [BucketService::class, ObjectService::class, ObjectStore::class])
internal class MultipartServiceTest : ServiceTestBase() {
  @MockitoBean
  private lateinit var multipartStore: MultipartStore

  @Autowired
  private lateinit var iut: MultipartService

  @Test
  fun testVerifyPartNumberLimits_success() {
    val partNumber = "1"
    iut.verifyPartNumberLimits(partNumber)
  }

  @Test
  fun testVerifyPartNumberLimits_tooSmallFailure() {
    val partNumber = "0"
    assertThatThrownBy { iut.verifyPartNumberLimits(partNumber) }
      .isEqualTo(S3Exception.INVALID_PART_NUMBER)
  }

  @Test
  fun testVerifyPartNumberLimits_tooLargeFailure() {
    val partNumber = "10001"
    assertThatThrownBy { iut.verifyPartNumberLimits(partNumber) }
      .isEqualTo(S3Exception.INVALID_PART_NUMBER)
  }

  @Test
  fun testVerifyPartNumberLimits_boundaryMax_success() {
    val partNumber = "10000"
    iut.verifyPartNumberLimits(partNumber)
  }

  @Test
  fun testVerifyPartNumberLimits_negativeNumberFailure() {
    val partNumber = "-1"
    assertThatThrownBy { iut.verifyPartNumberLimits(partNumber) }
      .isEqualTo(S3Exception.INVALID_PART_NUMBER)
  }

  @Test
  fun testVerifyMultipartParts_withRequestedParts_success() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    val requestedParts = from(parts)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts)
  }

  @Test
  fun testVerifyMultipartParts_withRequestedParts_wrongPartsFailure() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    val parts = givenParts(1, 1L)
    val requestedParts =
      listOf(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          "1L",
          1,
        ),
      )
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts) }
      .isEqualTo(S3Exception.INVALID_PART)
  }

  @Test
  fun testVerifyMultipartParts_withRequestedParts_wrongPartOrderFailure() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    val requestedParts = from(parts).toMutableList().also { it.reverse() }
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts) }
      .isEqualTo(S3Exception.INVALID_PART_ORDER)
  }

  private fun from(parts: List<Part>): List<CompletedPart> =
    parts.map { part ->
      CompletedPart(
        null,
        null,
        null,
        null,
        null,
        part.etag,
        part.partNumber,
      )
    }

  @Test
  fun testVerifyMultipartParts_onePart() {
    val bucketName = "bucketName"
    val id = UUID.randomUUID()
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(1, 1L)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, id, uploadId)
  }

  @Test
  fun testVerifyMultipartParts_twoParts() {
    val bucketName = "bucketName"
    val id = UUID.randomUUID()
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, id, uploadId)
  }

  @Test
  fun testVerifyMultipartParts_twoPartsFailure() {
    val bucketName = "bucketName"
    val id = UUID.randomUUID()
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, 1L)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(parts)
    assertThatThrownBy { iut.verifyMultipartParts(bucketName, id, uploadId) }
      .isEqualTo(S3Exception.ENTITY_TOO_SMALL)
  }

  @Test
  fun testVerifyMultipartUploadExists_failure() {
    val uploadId = UUID.randomUUID()
    val bucketName = "bucketName"
    whenever(bucketStore.getBucketMetadata(bucketName))
      .thenReturn(
        BucketMetadata(
          "bucketName",
          "null",
          null,
          null,
          null,
          null,
          Path.of(bucketName),
          "us-east-1",
          null,
          null,
        ),
      )
    whenever(
      multipartStore.getMultipartUpload(
        any<BucketMetadata>(),
        eq(uploadId),
        eq(false),
      ),
    ).thenThrow(IllegalArgumentException())
    assertThatThrownBy { iut.verifyMultipartUploadExists(bucketName, uploadId) }
      .isEqualTo(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
  }

  @Test
  fun testVerifyMultipartUploadExists_success() {
    val uploadId = UUID.randomUUID()
    val bucketName = "bucketName"
    iut.verifyMultipartUploadExists(bucketName, uploadId)
  }

  @Test
  fun testVerifyMultipartParts_withRequestedParts_keyNotFoundFailure() {
    val bucketName = "bucketName"
    val key = "missingKey"
    val uploadId = UUID.randomUUID()
    // create bucket but do not add the key to metadata so getID(key) returns null
    givenBucket(bucketName)

    val requestedParts = emptyList<CompletedPart>()

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts) }
      .isEqualTo(S3Exception.INVALID_PART)
  }

  @Test
  fun testVerifyMultipartParts_withRequestedParts_missingUploadedPartFailure() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    // Only part 1 was uploaded
    val uploadedParts = givenParts(1, MultipartService.MINIMUM_PART_SIZE)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(uploadedParts)

    // But request contains part 2 which does not exist in uploaded parts
    val requestedParts =
      listOf(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          "\"nonexistent-etag\"",
          2,
        ),
      )

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts) }
      .isEqualTo(S3Exception.INVALID_PART)
  }

  @Test
  fun testVerifyMultipartParts_idPath_noSuchUploadFailure() {
    val bucketName = "bucketName"
    val id = UUID.randomUUID()
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)

    // Simulate missing upload -> MultipartService should translate to NO_SUCH_UPLOAD_MULTIPART
    whenever(
      multipartStore.getMultipartUpload(
        eq(bucketMetadata),
        eq(uploadId),
        eq(false),
      ),
    ).thenThrow(IllegalArgumentException())

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, id, uploadId) }
      .isEqualTo(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
  }

  @Test
  fun `listMultipartUploads returns all uploads sorted by key`() {
    val bucketName = "bucket"
    val bucketMetadata = givenBucket(bucketName)

    val upload1 = givenMultipartUpload("b-key")
    val upload2 = givenMultipartUpload("a-key")
    whenever(multipartStore.listMultipartUploads(bucketMetadata, null)).thenReturn(listOf(upload1, upload2))

    val result = iut.listMultipartUploads(bucketName, null, null, null, 1000, null, null)

    assertThat(result.bucket).isEqualTo(bucketName)
    assertThat(result.multipartUploads).hasSize(2)
    // uploads are sorted by key
    assertThat(result.multipartUploads?.map { it.key }).containsExactly("a-key", "b-key")
  }

  @Test
  fun `listMultipartUploads truncates at maxUploads and sets truncated flag`() {
    val bucketName = "bucket"
    val bucketMetadata = givenBucket(bucketName)

    val uploads = (1..5).map { givenMultipartUpload("key-$it") }
    whenever(multipartStore.listMultipartUploads(bucketMetadata, null)).thenReturn(uploads)

    val result = iut.listMultipartUploads(bucketName, null, null, null, 3, null, null)

    assertThat(result.multipartUploads).hasSize(3)
    assertThat(result.isTruncated).isTrue()
    assertThat(result.nextKeyMarker).isEqualTo("key-3")
  }

  @Test
  fun `listMultipartUploads url-encodes keys when encodingType is url`() {
    val bucketName = "bucket"
    val bucketMetadata = givenBucket(bucketName)

    val upload = givenMultipartUpload("my key/with spaces")
    whenever(multipartStore.listMultipartUploads(bucketMetadata, null)).thenReturn(listOf(upload))

    val result = iut.listMultipartUploads(bucketName, null, "url", null, 1000, null, null)

    assertThat(result.multipartUploads?.get(0)?.key).isEqualTo("my%20key/with%20spaces")
  }

  @Test
  fun `listMultipartUploads collapses common prefixes with delimiter`() {
    val bucketName = "bucket"
    val bucketMetadata = givenBucket(bucketName)

    val uploads =
      listOf(
        givenMultipartUpload("a/b/key1"),
        givenMultipartUpload("a/b/key2"),
        givenMultipartUpload("c/key3"),
      )
    whenever(multipartStore.listMultipartUploads(bucketMetadata, null)).thenReturn(uploads)

    val result = iut.listMultipartUploads(bucketName, "/", null, null, 1000, null, null)

    assertThat(result.commonPrefixes?.map { it.prefix }).contains("a/")
  }

  @Test
  fun `getMultipartUploadParts returns parts filtered by partNumberMarker`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)

    val allParts = givenParts(5, MultipartService.MINIMUM_PART_SIZE)
    val upload = givenMultipartUpload(key)
    val uploadInfo = givenMultipartUploadInfo(upload)

    whenever(multipartStore.getMultipartUpload(bucketMetadata, uploadId, false)).thenReturn(upload)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(allParts)

    val result = iut.getMultipartUploadParts(bucketName, key, 1000, 2, uploadId)

    assertThat(result).isNotNull()
    // parts after partNumberMarker=2 → partNumbers 3, 4, 5
    assertThat(result!!.parts).hasSize(3)
    assertThat(result.parts?.map { it.partNumber }).containsExactly(3, 4, 5)
  }

  @Test
  fun `getMultipartUploadParts returns null when key not in bucket`() {
    val bucketName = "bucket"
    val key = "missing-key"
    val uploadId = UUID.randomUUID()
    givenBucket(bucketName)

    val result = iut.getMultipartUploadParts(bucketName, key, 1000, null, uploadId)

    assertThat(result).isNull()
  }

  @Test
  fun `getMultipartUploadParts truncates at maxParts and sets truncated flag`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)

    val allParts = givenParts(5, MultipartService.MINIMUM_PART_SIZE)
    val upload = givenMultipartUpload(key)

    whenever(multipartStore.getMultipartUpload(bucketMetadata, uploadId, false)).thenReturn(upload)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(allParts)

    val result = iut.getMultipartUploadParts(bucketName, key, 3, null, uploadId)

    assertThat(result).isNotNull()
    assertThat(result!!.parts).hasSize(3)
    assertThat(result.isTruncated).isTrue()
    assertThat(result.nextPartNumberMarker).isEqualTo(3)
  }

  private fun givenMultipartUpload(key: String): MultipartUpload =
    MultipartUpload(
      ChecksumAlgorithm.SHA256,
      ChecksumType.FULL_OBJECT,
      Date(),
      Initiator.DEFAULT_INITIATOR,
      key,
      Owner.DEFAULT_OWNER,
      StorageClass.STANDARD,
      UUID.randomUUID().toString(),
    )

  private fun givenMultipartUploadInfo(upload: MultipartUpload): MultipartUploadInfo =
    MultipartUploadInfo(
      upload,
      "application/octet-stream",
      emptyMap(),
      emptyMap(),
      emptyMap(),
      "bucket",
      StorageClass.STANDARD,
      null,
      null,
      null,
      ChecksumAlgorithm.SHA256,
      false,
    )
}
