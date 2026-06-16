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
package com.adobe.testing.s3mock.vectors.controller

import com.adobe.testing.s3mock.vectors.S3VectorsException
import com.adobe.testing.s3mock.vectors.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.DeleteVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.ListVectorBucketsRequest
import com.adobe.testing.s3mock.vectors.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.vectors.dto.VectorBucket
import com.adobe.testing.s3mock.vectors.dto.VectorBucketSummary
import com.adobe.testing.s3mock.vectors.service.VectorBucketService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus

/**
 * Direct unit tests for [VectorBucketController] — exercises controller logic without Spring MVC.
 * HTTP-level serialization is covered by the integration tests.
 */
internal class VectorBucketControllerTest {
  private val vectorBucketService: VectorBucketService = mock()
  private val iut = VectorBucketController(vectorBucketService)

  @Test
  fun `createVectorBucket returns 200 with ARN`() {
    whenever(vectorBucketService.createVectorBucket("my-bucket", null, emptyMap()))
      .thenReturn(CreateVectorBucketResponse("arn:aws:s3vectors:us-east-1:123:bucket/my-bucket"))

    val response = iut.createVectorBucket(CreateVectorBucketRequest("my-bucket", null, null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.vectorBucketArn).contains("my-bucket")
  }

  @Test
  fun `createVectorBucket with missing name throws ValidationException`() {
    assertThatThrownBy { iut.createVectorBucket(CreateVectorBucketRequest(null, null, null)) }
      .isInstanceOf(S3VectorsException::class.java)
      .hasMessageContaining("vectorBucketName is required")
  }

  @Test
  fun `createVectorBucket propagates ConflictException from service`() {
    whenever(vectorBucketService.createVectorBucket("existing", null, emptyMap()))
      .thenThrow(S3VectorsException.VECTOR_BUCKET_ALREADY_EXISTS)

    assertThatThrownBy { iut.createVectorBucket(CreateVectorBucketRequest("existing", null, null)) }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_ALREADY_EXISTS)
  }

  @Test
  fun `getVectorBucket returns 200 with bucket details`() {
    val bucket = VectorBucket("arn:aws:s3vectors:us-east-1:123:bucket/b", "b", 1.0, null)
    whenever(vectorBucketService.getVectorBucket("b")).thenReturn(GetVectorBucketResponse(bucket))

    val response = iut.getVectorBucket(GetVectorBucketRequest("b", null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.vectorBucket.vectorBucketName).isEqualTo("b")
  }

  @Test
  fun `getVectorBucket propagates NotFoundException from service`() {
    whenever(vectorBucketService.getVectorBucket("missing"))
      .thenThrow(S3VectorsException.VECTOR_BUCKET_NOT_FOUND)

    assertThatThrownBy { iut.getVectorBucket(GetVectorBucketRequest("missing", null)) }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_NOT_FOUND)
  }

  @Test
  fun `listVectorBuckets returns 200 with summaries`() {
    val summaries = listOf(VectorBucketSummary("arn1", "b1", 1.0), VectorBucketSummary("arn2", "b2", 2.0))
    whenever(vectorBucketService.listVectorBuckets(null, null, null))
      .thenReturn(ListVectorBucketsResponse(summaries, null))

    val response = iut.listVectorBuckets(null)

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.vectorBuckets).hasSize(2)
    assertThat(response.body!!.nextToken).isNull()
  }

  @Test
  fun `listVectorBuckets with null request returns empty list`() {
    whenever(vectorBucketService.listVectorBuckets(null, null, null))
      .thenReturn(ListVectorBucketsResponse(emptyList(), null))

    val response = iut.listVectorBuckets(null)

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.vectorBuckets).isEmpty()
  }

  @Test
  fun `deleteVectorBucket returns 200 on success`() {
    val response = iut.deleteVectorBucket(DeleteVectorBucketRequest("b", null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  fun `deleteVectorBucket propagates ConflictException from service`() {
    whenever(vectorBucketService.deleteVectorBucket("full"))
      .thenThrow(S3VectorsException.VECTOR_BUCKET_NOT_EMPTY)

    assertThatThrownBy { iut.deleteVectorBucket(DeleteVectorBucketRequest("full", null)) }
      .isEqualTo(S3VectorsException.VECTOR_BUCKET_NOT_EMPTY)
  }

  @Test
  fun `deleteVectorBucket with missing name throws ValidationException`() {
    assertThatThrownBy { iut.deleteVectorBucket(DeleteVectorBucketRequest(null, null)) }
      .isInstanceOf(S3VectorsException::class.java)
  }
}
