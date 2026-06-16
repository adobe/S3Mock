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
import com.adobe.testing.s3mock.vectors.dto.CreateIndexRequest
import com.adobe.testing.s3mock.vectors.dto.CreateIndexResponse
import com.adobe.testing.s3mock.vectors.dto.DeleteIndexRequest
import com.adobe.testing.s3mock.vectors.dto.GetIndexRequest
import com.adobe.testing.s3mock.vectors.dto.GetIndexResponse
import com.adobe.testing.s3mock.vectors.dto.IndexSummary
import com.adobe.testing.s3mock.vectors.dto.ListIndexesRequest
import com.adobe.testing.s3mock.vectors.dto.ListIndexesResponse
import com.adobe.testing.s3mock.vectors.dto.VectorIndex
import com.adobe.testing.s3mock.vectors.service.VectorIndexService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus

/**
 * Direct unit tests for [VectorIndexController] — exercises controller logic without Spring MVC.
 * HTTP-level serialization is covered by the integration tests.
 */
internal class VectorIndexControllerTest {
  private val vectorIndexService: VectorIndexService = mock()
  private val iut = VectorIndexController(vectorIndexService)

  @Test
  fun `createIndex returns 200 with index ARN`() {
    whenever(vectorIndexService.createIndex(any(), any(), any(), any(), any(), anyOrNull(), anyOrNull(), any()))
      .thenReturn(CreateIndexResponse("arn:aws:s3vectors:us-east-1:123:bucket/b/index/i"))

    val response = iut.createIndex(CreateIndexRequest("i", "float32", 128, "cosine", "b", null, null, null, null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.indexArn).contains("index/i")
  }

  @Test
  fun `createIndex with missing bucket throws ValidationException`() {
    assertThatThrownBy {
      iut.createIndex(CreateIndexRequest("i", "float32", 4, "cosine", null, null, null, null, null))
    }.isInstanceOf(S3VectorsException::class.java)
  }

  @Test
  fun `createIndex propagates ConflictException from service`() {
    whenever(vectorIndexService.createIndex(any(), any(), any(), any(), any(), anyOrNull(), anyOrNull(), any()))
      .thenThrow(S3VectorsException.INDEX_ALREADY_EXISTS)

    assertThatThrownBy {
      iut.createIndex(CreateIndexRequest("i", "float32", 4, "cosine", "b", null, null, null, null))
    }.isEqualTo(S3VectorsException.INDEX_ALREADY_EXISTS)
  }

  @Test
  fun `getIndex returns 200 with index details`() {
    val index = VectorIndex("arn1", "i", "b", "float32", 128, "cosine", 1.0, null, null)
    whenever(vectorIndexService.getIndex(anyOrNull(), any())).thenReturn(GetIndexResponse(index))

    val response = iut.getIndex(GetIndexRequest("i", "b", null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.index.indexName).isEqualTo("i")
  }

  @Test
  fun `getIndex propagates NotFoundException from service`() {
    whenever(vectorIndexService.getIndex(anyOrNull(), any())).thenThrow(S3VectorsException.INDEX_NOT_FOUND)

    assertThatThrownBy { iut.getIndex(GetIndexRequest("ghost", "b", null)) }
      .isEqualTo(S3VectorsException.INDEX_NOT_FOUND)
  }

  @Test
  fun `listIndexes returns 200 with index summaries`() {
    val summaries = listOf(IndexSummary("arn1", "idx-a", "b", 1.0), IndexSummary("arn2", "idx-b", "b", 2.0))
    whenever(vectorIndexService.listIndexes(any(), anyOrNull(), anyOrNull(), anyOrNull()))
      .thenReturn(ListIndexesResponse(summaries, null))

    val response = iut.listIndexes(ListIndexesRequest("b", null, null, null, null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
    assertThat(response.body!!.indexes).hasSize(2)
  }

  @Test
  fun `deleteIndex returns 200 on success`() {
    val response = iut.deleteIndex(DeleteIndexRequest("i", "b", null))

    assertThat(response.statusCode).isEqualTo(HttpStatus.OK)
  }

  @Test
  fun `deleteIndex propagates NotFoundException from service`() {
    whenever(vectorIndexService.deleteIndex(anyOrNull(), any())).thenThrow(S3VectorsException.INDEX_NOT_FOUND)

    assertThatThrownBy { iut.deleteIndex(DeleteIndexRequest("ghost", "b", null)) }
      .isEqualTo(S3VectorsException.INDEX_NOT_FOUND)
  }
}
