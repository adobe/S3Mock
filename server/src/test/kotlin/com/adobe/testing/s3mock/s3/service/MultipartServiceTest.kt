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
package com.adobe.testing.s3mock.s3.service

import com.adobe.testing.s3mock.s3.S3Exception
import com.adobe.testing.s3mock.s3.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.s3.dto.CompletedPart
import com.adobe.testing.s3mock.s3.dto.Initiator
import com.adobe.testing.s3mock.s3.dto.MultipartUpload
import com.adobe.testing.s3mock.s3.dto.Owner
import com.adobe.testing.s3mock.s3.dto.Part
import com.adobe.testing.s3mock.s3.dto.StorageClass
import com.adobe.testing.s3mock.s3.model.BucketMetadata
import com.adobe.testing.s3mock.s3.store.MultipartStore
import com.adobe.testing.s3mock.s3.store.ObjectStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Path
import java.time.Instant
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
  fun testVerifyMaxParts_success() {
    iut.verifyMaxParts(0)
    iut.verifyMaxParts(10)
  }

  @Test
  fun testVerifyMaxParts_failure() {
    assertThatThrownBy { iut.verifyMaxParts(-1) }
      .isEqualTo(S3Exception.INVALID_REQUEST_MAX_PARTS)
  }

  @Test
  fun testVerifyMaxUploads_success() {
    iut.verifyMaxUploads(0)
    iut.verifyMaxUploads(10)
  }

  @Test
  fun testVerifyMaxUploads_failure() {
    assertThatThrownBy { iut.verifyMaxUploads(-1) }
      .isEqualTo(S3Exception.INVALID_REQUEST_MAX_UPLOADS)
  }

  @Test
  fun `getMultipartUploadParts with max-parts zero returns an empty page`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val multipartUpload =
      MultipartUpload(
        null,
        null,
        Instant.now(),
        Initiator.DEFAULT_INITIATOR,
        key,
        Owner.DEFAULT_OWNER,
        StorageClass.STANDARD,
        uploadId.toString(),
      )
    whenever(multipartStore.getMultipartUpload(bucketMetadata, uploadId, false)).thenReturn(multipartUpload)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(givenParts(2, 1L))

    val result = iut.getMultipartUploadParts(bucketName, key, 0, null, uploadId)

    assertThat(result).isNotNull
    assertThat(result!!.parts).isEmpty()
    assertThat(result.isTruncated).isFalse
    assertThat(result.nextPartNumberMarker).isEqualTo(0)
  }

  @Test
  fun testListMultipartUploads_zeroMaxUploadsReturnsEmptyPage() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val multipartUpload =
      MultipartUpload(
        null,
        null,
        Instant.now(),
        Initiator.DEFAULT_INITIATOR,
        key,
        Owner.DEFAULT_OWNER,
        StorageClass.STANDARD,
        uploadId.toString(),
      )
    whenever(multipartStore.listMultipartUploads(bucketMetadata, null)).thenReturn(listOf(multipartUpload))

    val result = iut.listMultipartUploads(bucketName, null, null, null, 0, null, null)

    assertThat(result.multipartUploads).isEmpty()
    assertThat(result.isTruncated).isFalse
    assertThat(result.nextKeyMarker).isEqualTo("")
    assertThat(result.nextUploadIdMarker).isEqualTo("")
  }

  @Test
  fun `verifyMultipartParts accepts requested parts matching the uploaded parts`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    val requestedParts = from(parts)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts)
  }

  @Test
  fun `verifyMultipartParts fails with InvalidPart for a requested part with a wrong ETag`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
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
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, key, uploadId, requestedParts) }
      .isEqualTo(S3Exception.INVALID_PART)
  }

  @Test
  fun `verifyMultipartParts fails with InvalidPartOrder for requested parts out of order`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    val requestedParts = from(parts).toMutableList().also { it.reverse() }
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

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
  fun `verifyMultipartParts accepts a single part below the minimum size`() {
    val bucketName = "bucketName"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(1, 1L)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, uploadId)
  }

  @Test
  fun `verifyMultipartParts accepts parts of the minimum size`() {
    val bucketName = "bucketName"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, uploadId)
  }

  @Test
  fun `verifyMultipartParts fails with EntityTooSmall for a non-last part below the minimum size`() {
    val bucketName = "bucketName"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, 1L)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)
    assertThatThrownBy { iut.verifyMultipartParts(bucketName, uploadId) }
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
  fun `verifyMultipartParts accepts a key that is not in the bucket yet`() {
    val bucketName = "bucketName"
    val key = "newKey"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val parts = givenParts(2, MultipartService.MINIMUM_PART_SIZE)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(parts)

    iut.verifyMultipartParts(bucketName, key, uploadId, from(parts))
  }

  @Test
  fun `createMultipartUpload does not add the key to the bucket`() {
    val bucketName = "bucketName"
    val key = "key"
    val bucketMetadata = givenBucket(bucketName)
    whenever(
      multipartStore.createMultipartUpload(
        eq(bucketMetadata),
        eq(key),
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
    ).thenReturn(
      MultipartUpload(
        null,
        null,
        Instant.now(),
        Initiator.DEFAULT_INITIATOR,
        key,
        Owner.DEFAULT_OWNER,
        StorageClass.STANDARD,
        UUID.randomUUID().toString(),
      ),
    )

    iut.createMultipartUpload(
      bucketName,
      key,
      null,
      emptyMap(),
      Owner.DEFAULT_OWNER,
      Initiator.DEFAULT_INITIATOR,
      emptyMap(),
      emptyMap(),
      null,
      StorageClass.STANDARD,
      null,
      null,
    )

    verify(bucketStore, never()).addKeyToBucket(any(), any())
  }

  @Test
  fun `abortMultipartUpload does not remove the key from the bucket`() {
    val bucketName = "bucketName"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)

    iut.abortMultipartUpload(bucketName, uploadId)

    verify(multipartStore).abortMultipartUpload(bucketMetadata, uploadId)
    verify(bucketStore, never()).removeFromBucket(any(), any())
  }

  @Test
  fun `completeMultipartUpload adds a new key to the bucket`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = UUID.randomUUID()
    whenever(bucketStore.addKeyToBucket(key, bucketName)).thenReturn(id)
    whenCompleting(bucketMetadata, key, id, uploadId).thenReturn(completeResult(bucketName, key))

    iut.completeMultipartUpload(bucketName, key, uploadId, emptyList(), emptyMap(), "location", null, null, null)

    verify(bucketStore).addKeyToBucket(key, bucketName)
  }

  @Test
  fun `completeMultipartUpload reuses the id of an existing object`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    whenCompleting(bucketMetadata, key, id, uploadId).thenReturn(completeResult(bucketName, key))

    iut.completeMultipartUpload(bucketName, key, uploadId, emptyList(), emptyMap(), "location", null, null, null)

    verify(bucketStore, never()).addKeyToBucket(any(), any())
  }

  @Test
  fun `completeMultipartUpload removes a newly added key when completion fails`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = UUID.randomUUID()
    whenever(bucketStore.addKeyToBucket(key, bucketName)).thenReturn(id)
    whenCompleting(bucketMetadata, key, id, uploadId).thenThrow(S3Exception.BAD_CHECKSUM_CRC32)

    assertThatThrownBy {
      iut.completeMultipartUpload(bucketName, key, uploadId, emptyList(), emptyMap(), "location", null, null, null)
    }.isEqualTo(S3Exception.BAD_CHECKSUM_CRC32)

    verify(bucketStore).removeFromBucket(key, bucketName)
  }

  @Test
  fun `completeMultipartUpload keeps an existing key when completion fails`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
    whenCompleting(bucketMetadata, key, id, uploadId).thenThrow(S3Exception.BAD_CHECKSUM_CRC32)

    assertThatThrownBy {
      iut.completeMultipartUpload(bucketName, key, uploadId, emptyList(), emptyMap(), "location", null, null, null)
    }.isEqualTo(S3Exception.BAD_CHECKSUM_CRC32)

    verify(bucketStore, never()).removeFromBucket(any(), any())
  }

  private fun whenCompleting(
    bucketMetadata: BucketMetadata,
    key: String,
    id: UUID,
    uploadId: UUID,
  ) = whenever(
    multipartStore.completeMultipartUpload(
      eq(bucketMetadata),
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
  )

  private fun completeResult(
    bucketName: String,
    key: String,
  ) = CompleteMultipartUploadResult.from("location", bucketName, key, "etag", emptyMap(), null, null, null, null)

  @Test
  fun `verifyMultipartParts fails with InvalidPart for a requested part that was not uploaded`() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    // Only part 1 was uploaded
    val uploadedParts = givenParts(1, MultipartService.MINIMUM_PART_SIZE)
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, uploadId)).thenReturn(uploadedParts)

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
  fun `verifyMultipartParts fails with NoSuchUpload for an unknown upload`() {
    val bucketName = "bucketName"
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

    assertThatThrownBy { iut.verifyMultipartParts(bucketName, uploadId) }
      .isEqualTo(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
  }
}
