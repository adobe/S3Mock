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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.CopyPartResult
import com.adobe.testing.s3mock.dto.ErrorResponse
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
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.KmsKeyStore
import com.adobe.testing.s3mock.store.MultipartUploadInfo
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import org.apache.commons.lang3.tuple.Pair
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyList
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.util.MultiValueMap
import org.springframework.web.util.UriComponentsBuilder
import java.nio.file.Paths
import java.time.Instant
import java.util.Date
import java.util.UUID

@MockitoBean(types = [KmsKeyStore::class, ObjectController::class, BucketController::class])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class MultipartControllerTest : BaseControllerTest() {
  @MockitoBean
  private lateinit var bucketService: BucketService

  @MockitoBean
  private lateinit var multipartService: MultipartService

  @MockitoBean
  private lateinit var objectService: ObjectService

  @Autowired
  private lateinit var restTemplate: TestRestTemplate

  @Test
  @Throws(Exception::class)
  fun testCompleteMultipart_BadRequest_uploadTooSmall() {
    givenBucket()
    val parts = listOf(
      createPart(0, 5L),
      createPart(1, 5L)
    )

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    for (part in parts) {
      uploadRequest.addPart(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          part.etag,
          part.partNumber
        )
      )
    }

    val key = "sampleFile.txt"
    val uploadId = UUID.randomUUID()
    doThrow(S3Exception.ENTITY_TOO_SMALL)
      .whenever(multipartService)
      .verifyMultipartParts(
        eq(TEST_BUCKET_NAME),
        eq(key),
        eq(uploadId),
        anyList()
      )

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.ENTITY_TOO_SMALL)))
  }

  @Test
  @Throws(Exception::class)
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
        anyString(),
        eq(uploadId),
        anyList()
      )

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    for (part in parts) {
      uploadRequest.addPart(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          part.etag,
          part.partNumber
        )
      )
    }

    val key = "sampleFile.txt"

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART)))
  }

  @Test
  @Throws(Exception::class)
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
        anyList()
      )

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    for (part in requestParts) {
      uploadRequest.addPart(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          part.etag,
          part.partNumber
        )
      )
    }

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )
    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART)))
  }

  @Test
  @Throws(Exception::class)
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
        anyList()
      )

    val requestParts = listOf(
      createPart(1, 5L),
      createPart(0, 5L)
    )

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    for (part in requestParts) {
      uploadRequest.addPart(
        CompletedPart(
          null,
          null,
          null,
          null,
          null,
          part.etag,
          part.partNumber
        )
      )
    }

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }
    val uri = UriComponentsBuilder
      .fromUriString("/test-bucket/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()
    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_ORDER)))
  }

  @Test
  fun testCompleteMultipart_Ok_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "enc/key.txt"
    val uploadId = UUID.randomUUID()

    // parts
    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag2", 2))

    // object exists and matches
    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }

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
      null
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
        anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-server-side-encryption")).isEqualTo("AES256")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testCompleteMultipart_Ok_VersionIdHeaderWhenVersioned() {
    val bucketMeta = bucketMetadata(versioningEnabled = true)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "ver/key.txt"
    val uploadId = UUID.randomUUID()

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }

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
      null
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
        eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-version-id")).isEqualTo("v1")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testCompleteMultipart_Ok_NoVersionHeaderWhenNotVersioned() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "nover/key.txt"
    val uploadId = UUID.randomUUID()

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }

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
      null
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
        eq(TEST_BUCKET_NAME), eq(key), eq(uploadId), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()
      )
    ).thenReturn(result)

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-version-id")).isNull()
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testCompleteMultipart_PreconditionFailed() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "pre/key.txt"
    val uploadId = UUID.randomUUID()

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))

    val s3meta = s3ObjectMetadata(key, UUID.randomUUID().toString())
    whenever(objectService.getObject(TEST_BUCKET_NAME, key, null)).thenReturn(s3meta)

    // Simulate precondition failed
    doThrow(S3Exception.PRECONDITION_FAILED)
      .whenever(objectService)
      .verifyObjectMatching(anyOrNull(), anyOrNull(), anyOrNull(), anyOrNull(), eq(s3meta))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
      add("If-Match", "non-matching-etag")
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.PRECONDITION_FAILED)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.PRECONDITION_FAILED)))
  }

  @Test
  fun testCompleteMultipart_NoSuchBucket() {
    doThrow(S3Exception.NO_SUCH_BUCKET)
      .whenever(bucketService)
      .verifyBucketExists(TEST_BUCKET_NAME)

    val key = "missing-bucket/key.txt"
    val uploadId = UUID.randomUUID()

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testCompleteMultipart_NoSuchUpload() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val key = "no-upload/key.txt"
    val uploadId = UUID.randomUUID()

    doThrow(S3Exception.NO_SUCH_UPLOAD_MULTIPART)
      .whenever(multipartService)
      .verifyMultipartUploadExists(TEST_BUCKET_NAME, uploadId)

    val uploadRequest = CompleteMultipartUpload(ArrayList())
    uploadRequest.addPart(CompletedPart(null, null, null, null, null, "etag1", 1))

    val headers = HttpHeaders().apply {
      this.accept = listOf(MediaType.APPLICATION_XML)
      this.contentType = MediaType.APPLICATION_XML
    }

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity(MAPPER.writeValueAsString(uploadRequest), headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART)))
  }

  @Test
  fun testListMultipartUploads_Ok() {
    // Arrange
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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
      null, // keyMarker
      null, // delimiter
      null, // prefix
      null, // uploadIdMarker
      1000,
      false,
      null,
      null,
      uploads,
      emptyList(),
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
    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    // Assert
    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testListMultipartUploads_WithEncodingAndParams_PropagateCorrectly() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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
      keyMarker,
      delimiter,
      prefix,
      uploadIdMarker,
      maxUploads,
      false,
      null,
      null,
      uploads,
      emptyList(),
      encoding
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testListMultipartUploads_Pagination_ResponseFields() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploads = listOf(
      MultipartUpload(null, null, Date(), Owner.DEFAULT_OWNER, "k1", Owner.DEFAULT_OWNER, StorageClass.STANDARD, "u-1")
    )

    val result = ListMultipartUploadsResult(
      TEST_BUCKET_NAME,
      "k0",
      null,
      null,
      "u0",
      1,
      true,
      "k1",
      "u1",
      uploads,
      emptyList(),
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testAbortMultipartUpload_NoContent() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val key = "folder/name.txt"
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/$key")
      .queryParam("uploadId", uploadId)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.DELETE,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
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

    // Act
    val response = restTemplate.exchange(
      uri,
      HttpMethod.DELETE,
      HttpEntity.EMPTY,
      String::class.java
    )

    // Assert
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testAbortMultipartUpload_NoSuchUpload() {
    // Arrange
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    // Act
    val response = restTemplate.exchange(
      uri,
      HttpMethod.DELETE,
      HttpEntity.EMPTY,
      String::class.java
    )

    // Assert
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART)))
  }


  @Test
  fun testListParts_Ok() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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
      null
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testListParts_WithParams_PropagateCorrectly() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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
      null
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testListParts_Pagination_ResponseFields() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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
      null
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testListParts_NoSuchUpload() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.GET,
      HttpEntity.EMPTY,
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART)))
  }

  @Test
  fun testUploadPart_Ok_EtagReturned() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val temp = java.nio.file.Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair.of(temp, null))
    whenever(
      multipartService.putPart(eq(TEST_BUCKET_NAME), eq("my/key.txt"), eq(uploadId), eq("1"), eq(temp), any())
    ).thenReturn("etag-123")

    val headers = HttpHeaders()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("payload-bytes", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo("\"etag-123\"")
  }

  @Test
  fun testUploadPartCopy_Ok_VersionIdHeaderWhenVersioned() {
    val bucketMeta = bucketMetadata(versioningEnabled = true)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val s3meta = s3ObjectMetadata(
      key = "source/key.txt",
      id = UUID.randomUUID().toString(),
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
        eq("1"),
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity<MultiValueMap<String, String>>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-version-id")).isEqualTo("v1")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(copyResult))
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testUploadPartCopy_InvalidPartNumber_BadRequest() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_NUMBER)))
  }

  @Test
  fun testUploadPartCopy_SourceObjectNotFound() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_KEY)))
  }

  @Test
  fun testUploadPartCopy_PreconditionFailed() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.PRECONDITION_FAILED)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.PRECONDITION_FAILED)))
  }

  @Test
  fun testUploadPartCopy_NoVersionHeaderWhenNotVersioned() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val s3meta = s3ObjectMetadata(
      key = "source/key.txt",
      id = UUID.randomUUID().toString(),
      versionId = "v1"
    )
    whenever(objectService.verifyObjectExists(eq("source-bucket"), eq("source/key.txt"), eq("v1")))
      .thenReturn(s3meta)

    val copyResult = CopyPartResult(Date(), "etag-xyz")
    whenever(
      multipartService.copyPart(
        any(), any(), anyOrNull(), eq("1"), any(), any(), any(), any<Map<String, String>>(), any<String>()
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    // when versioning is disabled, controller should not echo x-amz-version-id
    assertThat(response.headers.getFirst("x-amz-version-id")).isNull()
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(copyResult))
  }

  @Test
  fun testUploadPartCopy_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

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
        eq("1"),
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-server-side-encryption")).isEqualTo("AES256")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(copyResult))
  }

  @Test
  fun testUploadPart_WithHeaderChecksum_VerifiedAndReturned() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)
    val uploadId = UUID.randomUUID()

    val temp = java.nio.file.Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair.of(temp, null))

    // when checksum headers are present, controller should call verifyChecksum and return header
    val checksum = "abc123checksum"
    val headers = HttpHeaders().apply {
      add("x-amz-checksum-algorithm", "SHA256")
      add("x-amz-checksum-sha256", checksum)
    }

    whenever(
      multipartService.putPart(eq(TEST_BUCKET_NAME), eq("my/key.txt"), eq(uploadId), eq("1"), eq(temp), any())
    ).thenReturn("etag-321")

    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("payload-bytes", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.eTag).isEqualTo("\"etag-321\"")
    // checksum header should be echoed
    assertThat(response.headers.getFirst("x-amz-checksum-sha256")).isEqualTo(checksum)
  }

  @Test
  fun testUploadPart_InvalidPartNumber_BadRequest() {
    // Arrange: toTempFile is called before validations
    val temp = java.nio.file.Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair.of(temp, null))

    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val uploadId = UUID.randomUUID()
    // Simulate invalid part number
    doThrow(S3Exception.INVALID_PART_NUMBER)
      .whenever(multipartService)
      .verifyPartNumberLimits("1")

    val headers = HttpHeaders()
    val uri = UriComponentsBuilder
      .fromUriString("/${TEST_BUCKET_NAME}/my/key.txt")
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", 1)
      .build()
      .toString()

    // Act
    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("payload-bytes", headers),
      String::class.java
    )

    // Assert
    assertThat(response.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.INVALID_PART_NUMBER)))
  }

  @Test
  fun testUploadPart_NoSuchBucket() {
    // toTempFile happens first
    val temp = java.nio.file.Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair.of(temp, null))

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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("payload-bytes", HttpHeaders()),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testUploadPart_NoSuchUpload() {
    val temp = java.nio.file.Files.createTempFile("junie", "part")
    whenever(multipartService.toTempFile(any(), any())).thenReturn(Pair.of(temp, null))

    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.PUT,
      HttpEntity("payload-bytes", HttpHeaders()),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_UPLOAD_MULTIPART)))
  }

  @Test
  fun testCreateMultipartUpload_Ok_ChecksumHeadersPropagated() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
    whenever(bucketService.verifyBucketExists(TEST_BUCKET_NAME)).thenReturn(bucketMeta)

    val result = InitiateMultipartUploadResult(TEST_BUCKET_NAME, "my/key.txt", "u-1")
    whenever(
      multipartService.createMultipartUpload(
        eq(TEST_BUCKET_NAME),
        eq("my/key.txt"),
        eq("application/octet-stream"),
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-checksum-algorithm")).isEqualTo("SHA256")
    assertThat(response.headers.getFirst("x-amz-checksum-type")).isEqualTo("FULL_OBJECT")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
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

    // Act
    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity("", HttpHeaders()),
      String::class.java
    )

    // Assert
    assertThat(response.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(from(S3Exception.NO_SUCH_BUCKET)))
  }

  @Test
  fun testCreateMultipartUpload_EncryptionHeadersEchoed() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.headers.getFirst("x-amz-server-side-encryption")).isEqualTo("AES256")
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testCreateMultipartUpload_StorageClass_Propagated() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity("", headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  @Test
  fun testCreateMultipartUpload_NoContentType_PassesNull() {
    val bucketMeta = bucketMetadata(versioningEnabled = false)
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

    val response = restTemplate.exchange(
      uri,
      HttpMethod.POST,
      HttpEntity<MultiValueMap<String, String>>(headers),
      String::class.java
    )

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body).isEqualTo(MAPPER.writeValueAsString(result))
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
  }

  companion object {
    private const val TEST_BUCKET_NAME = "test-bucket"
    private val TEST_BUCKET = Bucket(
      TEST_BUCKET_NAME,
      "us-east-1",
      Instant.now().toString(),
      Paths.get("/tmp/foo/1")
    )


    private fun createPart(partNumber: Int, size: Long): Part {
      return Part(partNumber, "someEtag$partNumber", Date(), size)
    }

    private fun from(e: S3Exception): ErrorResponse {
      return ErrorResponse(
        e.code,
        e.message,
        null,
        null
      )
    }

    private fun bucketMetadata(versioningEnabled: Boolean): BucketMetadata {
      val versioning = if (versioningEnabled) VersioningConfiguration(null, VersioningConfiguration.Status.ENABLED, null) else null
      return BucketMetadata(
        TEST_BUCKET_NAME,
        Instant.now().toString(),
        versioning,
        null,
        null,
        null,
        Paths.get("/tmp/foo/1"),
        "us-east-1",
        null,
        null
      )
    }

    private fun s3ObjectMetadata(
      key: String,
      id: String,
      versionId: String? = null
    ): S3ObjectMetadata {
      return S3ObjectMetadata(
        UUID.fromString(id),
        key,
        "0",
        Instant.now().toString(),
        "etag",
        "application/octet-stream",
        System.currentTimeMillis(),
        Paths.get("/tmp/foo/1/$key"),
        emptyMap(),
        emptyList(),
        null,
        null,
        Owner.DEFAULT_OWNER,
        emptyMap(),
        emptyMap(),
        null,
        null,
        null,
        null,
        versionId,
        false,
        ChecksumType.FULL_OBJECT
      )
    }
  }
}
