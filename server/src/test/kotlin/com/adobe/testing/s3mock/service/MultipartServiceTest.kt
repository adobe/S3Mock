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

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.store.ObjectStore
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.nio.file.Path
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
  fun testVerifyPartNumberLimits_noNumberFailure() {
    val partNumber = "NOT A NUMBER"
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
    val requestedParts = listOf(
      CompletedPart(
        null,
        null,
        null,
        null,
        null,
        "1L",
        1
      )
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

  private fun from(parts: List<Part>): List<CompletedPart> {
    return parts
      .stream()
      .map { part: Part ->
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          part.etag,
          part.partNumber
        )
      }.toList()
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
          null
        )
      )
    whenever(
      multipartStore.getMultipartUpload(
        ArgumentMatchers.any(
          BucketMetadata::class.java
        ), ArgumentMatchers.eq(uploadId)
      )
    )
      .thenThrow(IllegalArgumentException())
    assertThatThrownBy { iut.verifyMultipartUploadExists(bucketName, uploadId) }
      .isEqualTo(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
  }

  @Test
  fun testVerifyMultipartUploadExists_success() {
    val uploadId = UUID.randomUUID()
    val bucketName = "bucketName"
    iut.verifyMultipartUploadExists(bucketName, uploadId)
  }
}
