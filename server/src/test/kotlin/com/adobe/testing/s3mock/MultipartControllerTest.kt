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
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.KmsKeyStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyList
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.MockBeans
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.util.UriComponentsBuilder
import java.nio.file.Paths
import java.time.Instant
import java.util.Date

@MockBeans(
  MockBean(
    classes = [KmsKeyStore::class, ObjectService::class, ObjectController::class, BucketController::class]
  )
)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
internal class MultipartControllerTest : BaseControllerTest() {
  @MockBean
  private lateinit var bucketService: BucketService

  @MockBean
  private lateinit var multipartService: MultipartService

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
    val uploadId = "testUploadId"
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
    val uploadId = "testUploadId"

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
    val uploadId = "testUploadId"

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
    val uploadId = "testUploadId"

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

  private fun createPart(partNumber: Int, size: Long): Part {
    return Part(partNumber, "someEtag$partNumber", Date(), size)
  }

  private fun givenBucket() {
    whenever(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET)
    whenever(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true)
  }

  private fun from(e: S3Exception): ErrorResponse {
    return ErrorResponse(
      e.code,
      e.message,
      null,
      null
    )
  }

  companion object {
    private const val TEST_BUCKET_NAME = "test-bucket"
    private val TEST_BUCKET = Bucket(TEST_BUCKET_NAME, "us-east-1", Instant.now().toString(), Paths.get("/tmp/foo/1"))
  }
}
