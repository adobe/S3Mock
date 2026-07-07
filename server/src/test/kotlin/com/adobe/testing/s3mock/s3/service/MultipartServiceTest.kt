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
import org.mockito.kotlin.eq
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
  fun testGetMultipartUploadParts_zeroMaxPartsReturnsEmptyPage() {
    val bucketName = "bucketName"
    val key = "key"
    val uploadId = UUID.randomUUID()
    val bucketMetadata = givenBucket(bucketName)
    val id = bucketMetadata.addKey(key)
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
    whenever(multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)).thenReturn(givenParts(2, 1L))

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
}
