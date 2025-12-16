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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.CopyPartResult
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult
import com.adobe.testing.s3mock.dto.ListPartsResult
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.MultipartUploadInfo
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.isA
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.web.util.UriComponentsBuilder
import java.nio.file.Files
import java.util.Date
import java.util.UUID

@MockitoBean(types = [KmsKeyStore::class, ObjectController::class, BucketController::class])
@WebMvcTest
internal class MultipartControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var bucketService: BucketService

  @MockitoBean
  private lateinit var multipartService: MultipartService

  @MockitoBean
  private lateinit var objectService: ObjectService

  @Autowired
  private lateinit var mockMvc: MockMvc

  @Test
  fun testCompleteMultipart_BadRequest_uploadTooSmall() {
    givenBucket()
    val parts = listOf(
      createPart(0, 5L),
      createPart(1, 5L)
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(parts))

    val key = "sampleFile.txt"
    val uploadId = UUID.randomUUID()
    doThrow(S3Exception.ENTITY_TOO_SMALL)
      .whenever(multipartService)
      .verifyMultipartParts(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        isA<List<CompletedPart>>()
      )

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.ENTITY_TOO_SMALL))))
  }

  @Test
  fun testCompleteMultipart_BadRequest_uploadIdNotFound() {
    givenBucket()
    val uploadId = UUID.randomUUID()

    val parts = listOf(
      createPart(0, 5L),
      createPart(1, 5L)
    )

    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartParts(
        eq(TEST_BUCKET_NAME),
        isA<String>(),
        eq(uploadId),
        isA<List<CompletedPart>>()
      )

    val uploadRequest = CompleteMultipartUpload(completedParts(parts))

    val key = "sampleFile.txt"

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART))))
  }

  @Test
  fun testCompleteMultipart_BadRequest_partNotFound() {
    givenBucket()
    val key = "sampleFile.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(createPart(1, 5L))

    doThrow(S3Exception.INVALID_PART)
      .whenever(multipartService)
      .verifyMultipartParts(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        isA<List<CompletedPart>>()
      )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART))))
  }

  @Test
  fun testCompleteMultipart_BadRequest_invalidPartOrder() {
    givenBucket()

    val key = "sampleFile.txt"
    val uploadId = UUID.randomUUID()

    doThrow(S3Exception.INVALID_PART_ORDER)
      .whenever(multipartService)
      .verifyMultipartParts(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        isA<List<CompletedPart>>()
      )

    val requestParts = listOf(
      createPart(1, 5L),
      createPart(0, 5L)
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_ORDER))))
  }

  @Test
  fun testCompleteMultipart_Ok_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "enc/key.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
      createPart(0, 5L, "etag2")
    )

    // parts
    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    // object exists and matches
    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    // create result with encryption headers to be echoed
    val mpUpload = MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, key, Owner.DEFAULT_OWNER, StorageClass.STANDARD, uploadId.toString())
    val info = MultipartUploadInfo(
      mpUpload,
      "application/octet-stream",
      emptyMap(),
      emptyMap(),
      mapOf("x-amz-server-side-encryption" to "AES256"),
      TEST_BUCKET_NAME,
      StorageClass.STANDARD,
      emptyList(),
      null,
      ChecksumType.FULL_OBJECT,
      null,
    )
    val result = CompleteMultipartUploadResult.from(
      "http://localhost/${TEST_BUCKET_NAME}/$key",
      TEST_BUCKET_NAME,
      key,
      "etag-complete",
      info,
      null,
      ChecksumType.FULL_OBJECT,
      null,
      null
    )

    whenever(
      multipartService.completeMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-server-side-encryption", "AES256"))
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCompleteMultipart_Ok_VersionIdHeaderWhenVersioned() {
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED
    )
    val bucketMeta = bucketMetadata(versioningConfiguration = versioningConfiguration)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "ver/key.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    val mpUpload = MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, key, Owner.DEFAULT_OWNER, StorageClass.STANDARD, uploadId.toString())
    val info = MultipartUploadInfo(
      mpUpload,
      "application/octet-stream",
      emptyMap(),
      emptyMap(),
      emptyMap(),
      TEST_BUCKET_NAME,
      StorageClass.STANDARD,
      emptyList(),
      null,
      ChecksumType.FULL_OBJECT,
      null,
    )
    val result = CompleteMultipartUploadResult.from(
      "http://localhost/${TEST_BUCKET_NAME}/$key",
      TEST_BUCKET_NAME,
      key,
      "etag-complete",
      info,
      null,
      ChecksumType.FULL_OBJECT,
      null,
      "v1"
    )

    whenever(
      multipartService.completeMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-version-id", "v1"))
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCompleteMultipart_Ok_NoVersionHeaderWhenNotVersioned() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "nover/key.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    val mpUpload = MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, key, Owner.DEFAULT_OWNER, StorageClass.STANDARD, uploadId.toString())
    val info = MultipartUploadInfo(
      mpUpload,
      "application/octet-stream",
      emptyMap(),
      emptyMap(),
      emptyMap(),
      TEST_BUCKET_NAME,
      StorageClass.STANDARD,
      emptyList(),
      null,
      ChecksumType.FULL_OBJECT,
      null,
    )
    val result = CompleteMultipartUploadResult.from(
      "http://localhost/${TEST_BUCKET_NAME}/$key",
      TEST_BUCKET_NAME,
      key,
      "etag-complete",
      info,
      null,
      ChecksumType.FULL_OBJECT,
      null,
      "v1"
    )

    whenever(
      multipartService.completeMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        any(),
        anyOrNull(),
        any(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isOk)
      .andExpect(header().doesNotExist("x-amz-version-id"))
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCompleteMultipart_PreconditionFailed() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "pre/key.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    // Simulate precondition failed
    doThrow(S3Exception.PRECONDITION_FAILED)
      .whenever(objectService)
      .verifyObjectMatching(
        eq(TEST_BUCKET_NAME),
        eq(key),
        anyOrNull(),
        anyOrNull(),
      )

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .header("If-Match", "non-matching-etag")
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isPreconditionFailed)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.PRECONDITION_FAILED))))
  }

  @Test
  fun testCompleteMultipart_NoSuchBucket() {
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val key = "missing-bucket/key.txt"
    val uploadId = UUID.randomUUID()

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testCompleteMultipart_NoSuchUpload() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "no-upload/key.txt"
    val uploadId = UUID.randomUUID()

    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartUploadExists(TEST_BUCKET_NAME, uploadId, true)

    val requestParts = listOf(
      createPart(1, 5L, "etag1"),
    )

    val uploadRequest = CompleteMultipartUpload(completedParts(requestParts))

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .contentType(MediaType.APPLICATION_XML)
        .content(MAPPER.writeValueAsString(uploadRequest))
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART))))
  }

  @Test
  fun testListMultipartUploads_Ok() {
    // Arrange
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploads = listOf(
      MultipartUpload(
        null,
        null,
        Date(),
        Owner.DEFAULT_OWNER,
        "my/key.txt",
        Owner.DEFAULT_OWNER,
        StorageClass.STANDARD,
        "upload-1"
      )
    )

    val result = ListMultipartUploadsResult(
      TEST_BUCKET_NAME,
      emptyList(),
      null,
      null,
      false,
      null,
      1000,
      null,
      null,
      null,
      uploads,
      null
    )
    whenever(
      multipartService.listMultipartUploads(
        eq(TEST_BUCKET_NAME),
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
        eq(1000),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    // Act
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}")
      .queryParam("uploads", "")
      .build()
      .toString()
    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListMultipartUploads_WithEncodingAndParams_PropagateCorrectly() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val delimiter = "/"
    val encoding = "url"
    val keyMarker = "key-10"
    val maxUploads = 5
    val prefix = "pre"
    val uploadIdMarker = "u-marker"

    val uploads = listOf(
      MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, "pre/a.txt", Owner.DEFAULT_OWNER, StorageClass.STANDARD, "u-1")
    )
    val result = ListMultipartUploadsResult(
      TEST_BUCKET_NAME,
      emptyList(),
      delimiter,
      encoding,
      false,
      keyMarker,
      maxUploads,
      uploadIdMarker,
      null,
      prefix,
      uploads,
      null
    )

    whenever(
      multipartService.listMultipartUploads(
        eq(TEST_BUCKET_NAME),
        eq(delimiter),
        eq(encoding),
        eq(keyMarker),
        eq(maxUploads),
        eq(prefix),
        eq(uploadIdMarker)
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}")
      .queryParam("uploads", "")
      .queryParam("delimiter", delimiter)
      .queryParam("encoding-type", encoding)
      .queryParam("key-marker", keyMarker)
      .queryParam("max-uploads", maxUploads)
      .queryParam("prefix", prefix)
      .queryParam("upload-id-marker", uploadIdMarker)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListMultipartUploads_Pagination_ResponseFields() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploads = listOf(
      MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, "k1", Owner.DEFAULT_OWNER, StorageClass.STANDARD, "u-1")
    )

    val result = ListMultipartUploadsResult(
      TEST_BUCKET_NAME,
      listOf(),
      "k0",
      null,
      true,
      null,
      1,
      "u0",
      "k1",
      "u1",
      uploads,
      null
    )

    whenever(
      multipartService.listMultipartUploads(
        eq(TEST_BUCKET_NAME), anyOrNull(), anyOrNull(), anyOrNull(), eq(1), anyOrNull(), anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}")
      .queryParam("uploads", "")
      .queryParam("max-uploads", 1)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListMultipartUploads_NoSuchBucket() {
    // Simulate bucket missing
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testAbortMultipartUpload_NoContent() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val key = "folder/name.txt"
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      delete(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNoContent)
  }

  @Test
  fun testAbortMultipartUpload_NoSuchBucket() {
    // Arrange: bucket does not exist
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val key = "some/key.txt"
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      delete(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testAbortMultipartUpload_NoSuchUpload() {
    // Arrange
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploadId = UUID.randomUUID()
    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartUploadExists(TEST_BUCKET_NAME, uploadId)

    val key = "folder/name.txt"
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      delete(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART))))
  }


  @Test
  fun testListParts_Ok() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val parts = listOf(createPart(1, 5L), createPart(2, 6L))
    val result = ListPartsResult(
      TEST_BUCKET_NAME,
      null,
      null,
      Owner.DEFAULT_OWNER,
      false,
      "my/key.txt",
      1000,
      null,
      Owner.DEFAULT_OWNER,
      parts,
      null,
      StorageClass.STANDARD,
      uploadId.toString(),
    )
    whenever(
      multipartService.getMultipartUploadParts(
        any(),
        any(),
        any(),
        anyOrNull(),
        eq(uploadId)
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListParts_WithParams_PropagateCorrectly() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val maxParts = 5
    val partNumberMarker = 3

    val parts = listOf(createPart(4, 5L), createPart(5, 6L))
    val result = ListPartsResult(
      TEST_BUCKET_NAME,
      null,
      null,
      Owner.DEFAULT_OWNER,
      false,
      "my/key.txt",
      maxParts,
      null,
      Owner.DEFAULT_OWNER,
      parts,
      partNumberMarker,
      StorageClass.STANDARD,
      uploadId.toString(),
    )

    whenever(
      multipartService.getMultipartUploadParts(
        eq(TEST_BUCKET_NAME),
        eq("my/key.txt"),
        eq(maxParts),
        eq(partNumberMarker),
        eq(uploadId)
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("max-parts", maxParts)
      .queryParam("part-number-marker", partNumberMarker)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListParts_Pagination_ResponseFields() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val maxParts = 1
    val partNumberMarker = 1
    val nextPartNumberMarker = 2

    val parts = listOf(createPart(2, 6L))
    val result = ListPartsResult(
      TEST_BUCKET_NAME,
      null,
      null,
      Owner.DEFAULT_OWNER,
      true,
      "my/key.txt",
      maxParts,
      nextPartNumberMarker,
      Owner.DEFAULT_OWNER,
      parts,
      partNumberMarker,
      StorageClass.STANDARD,
      uploadId.toString(),
    )

    whenever(
      multipartService.getMultipartUploadParts(
        eq(TEST_BUCKET_NAME),
        eq("my/key.txt"),
        eq(maxParts),
        eq(partNumberMarker),
        eq(uploadId)
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("max-parts", maxParts)
      .queryParam("part-number-marker", partNumberMarker)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testListParts_NoSuchBucket() {
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testListParts_NoSuchUpload() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploadId = UUID.randomUUID()
    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartUploadExists(TEST_BUCKET_NAME, uploadId)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    mockMvc.perform(
      get(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART))))
  }

  @Test
  fun testUploadPart_Ok_EtagReturned() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)
    val uploadId = UUID.randomUUID()

    val temp = Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair(temp, null))
    whenever(
      multipartService.putPart(eq(TEST_BUCKET_NAME), eq("my/key.txt"), eq(uploadId), eq(1), eq(temp), any())
    ).thenReturn("etag-123")

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .content("payload-bytes")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"etag-123\""))
  }

  @Test
  fun testUploadPartCopy_Ok_VersionIdHeaderWhenVersioned() {
    val versioningConfiguration = VersioningConfiguration(
      VersioningConfiguration.MFADelete.DISABLED,
      VersioningConfiguration.Status.ENABLED
    )
    val bucketMeta = bucketMetadata(versioningConfiguration = versioningConfiguration)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    val s3meta = s3ObjectMetadata(
      key = "source/key.txt",
      versionId = "v1"
    )
    whenever(
      objectService.verifyObjectExists(
        eq("source-bucket"),
        eq("source/key.txt"),
        eq("v1")
      )
    ).thenReturn(s3meta)

    val copyResult = CopyPartResult(Date(), "etag-xyz")
    whenever(
      multipartService.copyPart(
        any(),
        any(),
        anyOrNull(),
        eq(1),
        any(),
        any(),
        any(),
        any<Map<String, String>>(),
        any<String>()
      )
    ).thenReturn(copyResult)

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt?versionId=v1")
      // Optional: no range or match headers
    }

    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-version-id", "v1"))
      .andExpect(content().string(MAPPER.writeValueAsString(copyResult)))
  }

  @Test
  fun testUploadPartCopy_NoSuchBucket() {
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt")
    }
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testUploadPartCopy_InvalidPartNumber_BadRequest() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    doThrow(S3Exception.INVALID_PART_NUMBER)
      .whenever(multipartService)
      .verifyPartNumberLimits("1")

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt")
    }
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_NUMBER))))
  }

  @Test
  fun testUploadPartCopy_SourceObjectNotFound() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    doThrow(S3Exception.NO_SUCH_KEY)
      .whenever(objectService)
      .verifyObjectExists(eq("source-bucket"), eq("source/key.txt"), anyOrNull())

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt")
    }
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_KEY))))
  }

  @Test
  fun testUploadPartCopy_PreconditionFailed() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    val s3meta = s3ObjectMetadata("source/key.txt", UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists(eq("source-bucket"), eq("source/key.txt"), anyOrNull()))
      .thenReturn(s3meta)

    // Simulate precondition failed on matching
    doThrow(S3Exception.PRECONDITION_FAILED)
      .whenever(objectService)
      .verifyObjectMatchingForCopy(
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
        anyOrNull(),
        eq(s3meta)
      )

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt")
      add("x-amz-copy-source-if-match", "etag-not-matching")
    }
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isPreconditionFailed)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.PRECONDITION_FAILED))))
  }

  @Test
  fun testUploadPartCopy_NoVersionHeaderWhenNotVersioned() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    val s3meta = s3ObjectMetadata(
      key = "source/key.txt",
      versionId = "v1"
    )
    whenever(objectService.verifyObjectExists(eq("source-bucket"), eq("source/key.txt"), eq("v1")))
      .thenReturn(s3meta)

    val copyResult = CopyPartResult(Date(), "etag-xyz")
    whenever(
      multipartService.copyPart(
        any(), any(), anyOrNull(), eq(1), any(), any(), any(), any<Map<String, String>>(), any<String>()
      )
    ).thenReturn(copyResult)

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt?versionId=v1")
    }
    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(header().doesNotExist("x-amz-version-id"))
      .andExpect(content().string(MAPPER.writeValueAsString(copyResult)))
  }

  @Test
  fun testUploadPartCopy_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)

    val s3meta = s3ObjectMetadata("source/key.txt", UUID.randomUUID().toString())
    whenever(objectService.verifyObjectExists(eq("source-bucket"), eq("source/key.txt"), anyOrNull()))
      .thenReturn(s3meta)

    val copyResult = CopyPartResult(Date(), "etag-enc")
    val uploadId = UUID.randomUUID()
    whenever(
      multipartService.copyPart(
        eq("source-bucket"),
        eq("source/key.txt"),
        anyOrNull(),
        eq(1),
        eq(TEST_BUCKET_NAME),
        eq("dest/key.txt"),
        eq(uploadId),
        eq(mapOf("x-amz-server-side-encryption" to "AES256")),
        anyOrNull<String>()
      )
    ).thenReturn(copyResult)

    val headers = HttpHeaders().apply {
      add("x-amz-copy-source", "/source-bucket/source/key.txt")
      // Only headers starting with x-amz-server-side-encryption are echoed
      add("x-amz-server-side-encryption", "AES256")
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/dest/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-server-side-encryption", "AES256"))
      .andExpect(content().string(MAPPER.writeValueAsString(copyResult)))
  }

  @Test
  fun testUploadPart_WithHeaderChecksum_VerifiedAndReturned() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    whenever(multipartService.verifyPartNumberLimits("1")).thenReturn(1)
    val uploadId = UUID.randomUUID()

    val temp = Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair(temp, null))

    // when checksum headers are present, controller should call verifyChecksum and return header
    val checksum = "abc123checksum"
    val headers = HttpHeaders().apply {
      add("x-amz-checksum-algorithm", "SHA256")
      add("x-amz-checksum-sha256", checksum)
    }

    whenever(
      multipartService.putPart(eq(TEST_BUCKET_NAME), eq("my/key.txt"), eq(uploadId), eq(1), eq(temp), any())
    ).thenReturn("etag-321")

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
        .content("payload-bytes")
    )
      .andExpect(status().isOk)
      .andExpect(header().string(HttpHeaders.ETAG, "\"etag-321\""))
      .andExpect(header().string("x-amz-checksum-sha256", checksum))
  }

  @Test
  fun testUploadPart_InvalidPartNumber_BadRequest() {
    // Arrange: toTempFile is called before validations
    val temp = Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair(temp, null))

    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploadId = UUID.randomUUID()
    // Simulate invalid part number
    doThrow(S3Exception.INVALID_PART_NUMBER)
      .whenever(multipartService)
      .verifyPartNumberLimits("1")

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .content("payload-bytes")
    )
      .andExpect(status().isBadRequest)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_NUMBER))))
  }

  @Test
  fun testUploadPart_NoSuchBucket() {
    // toTempFile happens first
    val temp = Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair(temp, null))

    // bucket missing
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val uploadId = UUID.randomUUID()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .content("payload-bytes")
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testUploadPart_NoSuchUpload() {
    val temp = Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair(temp, null))

    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploadId = UUID.randomUUID()
    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartUploadExists(TEST_BUCKET_NAME, uploadId)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    mockMvc.perform(
      put(uri)
        .accept(MediaType.APPLICATION_XML)
        .content("payload-bytes")
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART))))
  }

  @Test
  fun testCreateMultipartUpload_Ok_ChecksumHeadersPropagated() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val result = InitiateMultipartUploadResult(TEST_BUCKET_NAME, "my/key.txt", "u-1")
    whenever(
      multipartService.createMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq("my/key.txt"),
        argThat<String> { this.startsWith("application/octet-stream") },
        anyOrNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(Owner.DEFAULT_OWNER),
        anyOrNull<Map<String, String>>(),
        anyOrNull<Map<String, String>>(),
        anyOrNull<List<Tag>>(),
        eq(StorageClass.STANDARD),
        eq(ChecksumType.FULL_OBJECT),
        eq(ChecksumAlgorithm.SHA256)
      )
    ).thenReturn(result)

    val headers = HttpHeaders().apply {
      // supply checksum type and algorithm headers
      add("x-amz-checksum-type", "FULL_OBJECT")
      add("x-amz-checksum-algorithm", "SHA256")
      add("Content-Type", "application/octet-stream")
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-checksum-algorithm", "SHA256"))
      .andExpect(header().string("x-amz-checksum-type", "FULL_OBJECT"))
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCreateMultipartUpload_NoSuchBucket() {
    // Arrange: bucket does not exist
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
    )
      .andExpect(status().isNotFound)
      .andExpect(content().string(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET))))
  }

  @Test
  fun testCreateMultipartUpload_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val result = InitiateMultipartUploadResult(TEST_BUCKET_NAME, "enc/key.txt", "u-enc-1")
    whenever(
      multipartService.createMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq("enc/key.txt"),
        anyOrNull(),
        anyOrNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(Owner.DEFAULT_OWNER),
        anyOrNull<Map<String, String>>(),
        eq(mapOf("x-amz-server-side-encryption" to "AES256")),
        anyOrNull<List<Tag>>(),
        eq(StorageClass.STANDARD),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val headers = HttpHeaders().apply {
      add("x-amz-server-side-encryption", "AES256")
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/enc/key.txt")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(header().string("x-amz-server-side-encryption", "AES256"))
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCreateMultipartUpload_StorageClass_Propagated() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val result = InitiateMultipartUploadResult(TEST_BUCKET_NAME, "sc/key.txt", "u-sc-1")
    whenever(
      multipartService.createMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq("sc/key.txt"),
        anyOrNull(),
        anyOrNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(Owner.DEFAULT_OWNER),
        anyOrNull<Map<String, String>>(),
        anyOrNull<Map<String, String>>(),
        anyOrNull<List<Tag>>(),
        eq(StorageClass.GLACIER),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val headers = HttpHeaders().apply {
      add("x-amz-storage-class", "GLACIER")
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/sc/key.txt")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  @Test
  fun testCreateMultipartUpload_NoContentType_PassesNull() {
    val bucketMeta = bucketMetadata()
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val result = InitiateMultipartUploadResult(TEST_BUCKET_NAME, "noct/key.txt", "u-noct-1")
    whenever(
      multipartService.createMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq("noct/key.txt"),
        eq(null),
        anyOrNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(Owner.DEFAULT_OWNER),
        anyOrNull<Map<String, String>>(),
        anyOrNull<Map<String, String>>(),
        anyOrNull<List<Tag>>(),
        eq(StorageClass.STANDARD),
        anyOrNull(),
        anyOrNull()
      )
    ).thenReturn(result)

    val headers = HttpHeaders() // no Content-Type header

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/noct/key.txt")
      .queryParam("uploads", "")
      .build()
      .toString()

    mockMvc.perform(
      post(uri)
        .accept(MediaType.APPLICATION_XML)
        .headers(headers)
    )
      .andExpect(status().isOk)
      .andExpect(content().string(MAPPER.writeValueAsString(result)))
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
  }

  companion object {
    private fun createPart(partNumber: Int, size: Long, etag: String = "someEtag$partNumber"): Part {
      return Part(partNumber, etag, Date(), size)
    }

    private fun completedParts(parts: List<Part>): List<CompletedPart> {
      return parts.asSequence()
        .map {
          CompletedPart(
            null,
            null,
            null,
            null,
            null,
            it.etag,
            it.partNumber
          )
        }
        .toList()
    }
  }
}
