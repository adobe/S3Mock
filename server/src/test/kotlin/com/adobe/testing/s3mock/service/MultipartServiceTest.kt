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
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Files
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

  // --- putPart ---

  @Test
  fun `putPart returns etag when key exists`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMeta = givenBucket(bucketName)
    val id = bucketMeta.addKey(key)
    val tempFile = Files.createTempFile("part", ".tmp")
    try {
      whenever(multipartStore.putPart(any(), eq(id), eq(uploadId), eq(1), any(), any())).thenReturn("\"etag-1\"")

      val result = iut.putPart(bucketName, key, uploadId, 1, tempFile, emptyMap())

      assertThat(result).isEqualTo("\"etag-1\"")
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  @Test
  fun `putPart returns null when key does not exist`() {
    val bucketName = "bucket"
    givenBucket(bucketName)
    val tempFile = Files.createTempFile("part", ".tmp")
    try {
      val result = iut.putPart(bucketName, "missing", UUID.randomUUID(), 1, tempFile, emptyMap())
      assertThat(result).isNull()
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  // --- getMultipartUploadParts ---

  @Test
  fun `getMultipartUploadParts returns result with parts when key exists`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMeta = givenBucket(bucketName)
    val id = bucketMeta.addKey(key)
    val parts = givenParts(3, MultipartService.MINIMUM_PART_SIZE)
    val upload = multipartUpload(key, uploadId)
    whenever(multipartStore.getMultipartUpload(bucketMeta, uploadId, false)).thenReturn(upload)
    whenever(multipartStore.getMultipartUploadParts(bucketMeta, id, uploadId)).thenReturn(parts)

    val result = iut.getMultipartUploadParts(bucketName, key, 100, null, uploadId)

    assertThat(result).isNotNull()
    assertThat(result!!.parts).hasSize(3)
    assertThat(result.bucket).isEqualTo(bucketName)
    assertThat(result.key).isEqualTo(key)
  }

  @Test
  fun `getMultipartUploadParts returns null when key does not exist`() {
    val bucketName = "bucket"
    givenBucket(bucketName)

    val result = iut.getMultipartUploadParts(bucketName, "missing", 100, null, UUID.randomUUID())
    assertThat(result).isNull()
  }

  @Test
  fun `getMultipartUploadParts truncates when maxParts exceeded`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMeta = givenBucket(bucketName)
    val id = bucketMeta.addKey(key)
    val parts = givenParts(5, MultipartService.MINIMUM_PART_SIZE)
    val upload = multipartUpload(key, uploadId)
    whenever(multipartStore.getMultipartUpload(bucketMeta, uploadId, false)).thenReturn(upload)
    whenever(multipartStore.getMultipartUploadParts(bucketMeta, id, uploadId)).thenReturn(parts)

    val result = iut.getMultipartUploadParts(bucketName, key, 2, null, uploadId)

    assertThat(result).isNotNull()
    assertThat(result!!.parts).hasSize(2)
    assertThat(result.isTruncated).isTrue()
  }

  // --- abortMultipartUpload ---

  @Test
  fun `abortMultipartUpload calls store and removes key`() {
    val bucketName = "bucket"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMeta = givenBucket(bucketName)
    bucketMeta.addKey(key)

    iut.abortMultipartUpload(bucketName, key, uploadId)

    verify(multipartStore).abortMultipartUpload(eq(bucketMeta), any(), eq(uploadId))
    verify(bucketStore).removeFromBucket(key, bucketName)
  }

  // --- completeMultipartUpload ---

  @Test
  fun `completeMultipartUpload returns null when key does not exist`() {
    val bucketName = "bucket"
    givenBucket(bucketName)

    val result =
      iut.completeMultipartUpload(
        bucketName,
        "missing",
        UUID.randomUUID(),
        emptyList(),
        emptyMap(),
        "http://localhost/bucket/missing",
        null,
        null,
        null,
      )
    assertThat(result).isNull()
  }

  @Test
  fun `completeMultipartUpload delegates to store when key exists`() {
    val bucketName = "bucket-complete-exists"
    val key = "key-complete"
    val uploadId = UUID.randomUUID()
    val bucketMeta = metadataFrom(bucketName)
    val id = bucketMeta.addKey(key)
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(bucketMeta)
    val uploadInfo = multipartUploadInfo(bucketName, key, uploadId)
    val expected =
      com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult(
        bucket = bucketName,
        etag = "\"etag\"",
        key = key,
        location = "http://localhost",
        multipartUploadInfo = uploadInfo,
        versionId = null,
      )
    whenever(multipartStore.getMultipartUploadInfo(bucketMeta, uploadId)).thenReturn(uploadInfo)
    whenever(
      multipartStore.completeMultipartUpload(
        eq(bucketMeta),
        eq(key),
        eq(id),
        eq(uploadId),
        any(),
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
      ),
    ).thenReturn(expected)

    val result =
      iut.completeMultipartUpload(
        bucketName,
        key,
        uploadId,
        emptyList(),
        emptyMap(),
        "http://localhost/$bucketName/$key",
        null,
        null,
        null,
      )
    assertThat(result).isEqualTo(expected)
  }

  // --- createMultipartUpload ---

  @Test
  fun `createMultipartUpload returns initiate result with bucket and key`() {
    val bucketName = "bucket-create-mpu"
    val key = "key-mpu"
    val uploadId = UUID.randomUUID()
    val bucketMeta = metadataFrom(bucketName)
    whenever(bucketStore.getBucketMetadata(bucketName)).thenReturn(bucketMeta)
    val upload = multipartUpload(key, uploadId)

    whenever(bucketStore.addKeyToBucket(eq(key), eq(bucketName))).thenAnswer {
      bucketMeta.addKey(key)
    }
    whenever(
      multipartStore.createMultipartUpload(
        eq(bucketMeta),
        eq(key),
        any(),
        anyOrNull(),
        any(),
        any(),
        any(),
        any(),
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
      ),
    ).thenReturn(upload)

    val result =
      iut.createMultipartUpload(
        bucketName,
        key,
        "text/plain",
        emptyMap(),
        Owner("0"),
        Initiator("0", "root"),
        emptyMap(),
        emptyMap(),
        null,
        StorageClass.STANDARD,
        null,
        null,
      )

    assertThat(result.bucketName).isEqualTo(bucketName)
    assertThat(result.fileName).isEqualTo(key)
  }

  // --- listMultipartUploads ---

  @Test
  fun `listMultipartUploads returns uploads in bucket`() {
    val bucketName = "bucket"
    val bucketMeta = givenBucket(bucketName)
    val upload1 = multipartUpload("key1", UUID.randomUUID())
    val upload2 = multipartUpload("key2", UUID.randomUUID())
    whenever(multipartStore.listMultipartUploads(bucketMeta, null)).thenReturn(listOf(upload1, upload2))

    val result =
      iut.listMultipartUploads(
        bucketName,
        null,
        null,
        null,
        100,
        null,
        null,
      )

    assertThat(result.multipartUploads).hasSize(2)
    assertThat(result.bucket).isEqualTo(bucketName)
  }

  @Test
  fun `listMultipartUploads truncates when maxUploads exceeded`() {
    val bucketName = "bucket"
    val bucketMeta = givenBucket(bucketName)
    val uploads = (1..5).map { multipartUpload("key$it", UUID.randomUUID()) }
    whenever(multipartStore.listMultipartUploads(bucketMeta, null)).thenReturn(uploads)

    val result =
      iut.listMultipartUploads(
        bucketName,
        null,
        null,
        null,
        2,
        null,
        null,
      )

    assertThat(result.multipartUploads).hasSize(2)
    assertThat(result.isTruncated).isTrue()
    assertThat(result.nextKeyMarker).isNotNull()
  }

  private fun multipartUpload(
    key: String,
    uploadId: UUID,
  ): MultipartUpload =
    MultipartUpload(
      null,
      null,
      Date(),
      Initiator("0", "root"),
      key,
      Owner("0"),
      StorageClass.STANDARD,
      uploadId.toString(),
    )

  private fun multipartUploadInfo(
    bucket: String,
    key: String,
    uploadId: UUID,
  ): MultipartUploadInfo =
    MultipartUploadInfo(
      multipartUpload(key, uploadId),
      "text/plain",
      emptyMap(),
      emptyMap(),
      emptyMap(),
      bucket,
      StorageClass.STANDARD,
      null,
      null,
      null,
      null,
    )
}
