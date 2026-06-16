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
package com.adobe.testing.s3mock.vectors.service

import com.adobe.testing.s3mock.vectors.S3VectorsException
import com.adobe.testing.s3mock.vectors.dto.GetOutputVector
import com.adobe.testing.s3mock.vectors.dto.GetVectorsResponse
import com.adobe.testing.s3mock.vectors.dto.ListOutputVector
import com.adobe.testing.s3mock.vectors.dto.ListVectorsResponse
import com.adobe.testing.s3mock.vectors.dto.PutInputVector
import com.adobe.testing.s3mock.vectors.dto.VectorData
import com.adobe.testing.s3mock.vectors.store.VectorStore

open class VectorService(
  private val vectorIndexService: VectorIndexService,
  private val vectorStore: VectorStore,
) : VectorServiceBase() {
  fun putVectors(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    vectors: List<PutInputVector>,
  ) {
    val (bucketName, indexName) = vectorIndexService.resolveIndexId(bucketNameOrArn, indexNameOrArn)
    val indexMeta = vectorIndexService.requireIndex(bucketName, indexName)

    for ((key1, data, metadata) in vectors) {
      val key = key1 ?: throw S3VectorsException.validation("Vector key is required.")
      val floats =
        data
          ?.float32
          ?.map { it.toFloat() }
          ?.toFloatArray()
          ?: throw S3VectorsException.validation("Vector data is required.")
      validateVector(floats, indexMeta.dimension, indexMeta.distanceMetric)
      vectorStore.putVector(bucketName, indexName, key, floats, metadata)
    }
  }

  fun getVectors(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    keys: List<String>,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): GetVectorsResponse {
    val (bucketName, indexName) = vectorIndexService.resolveIndexId(bucketNameOrArn, indexNameOrArn)
    vectorIndexService.requireIndex(bucketName, indexName)

    val results =
      keys.map { key ->
        val stored = vectorStore.getVector(bucketName, indexName, key, returnData, returnMetadata)
        GetOutputVector(
          key = key,
          data = if (returnData && stored?.floats != null) VectorData(stored.floats.map { it.toDouble() }) else null,
          metadata = if (returnMetadata) stored?.metadata else null,
        )
      }
    return GetVectorsResponse(results)
  }

  fun listVectors(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    maxResults: Int?,
    nextToken: String?,
    returnData: Boolean,
    returnMetadata: Boolean,
    segmentCount: Int?,
    segmentIndex: Int?,
  ): ListVectorsResponse {
    val max = (maxResults ?: 500).coerceIn(1, 1000)
    val (bucketName, indexName) = vectorIndexService.resolveIndexId(bucketNameOrArn, indexNameOrArn)
    vectorIndexService.requireIndex(bucketName, indexName)

    var all = vectorStore.listVectors(bucketName, indexName, returnData, returnMetadata)

    if (segmentCount != null || segmentIndex != null) {
      if (segmentCount == null || segmentIndex == null) {
        throw S3VectorsException.validation("segmentCount and segmentIndex must both be specified.")
      }
      if (segmentCount <= 0) throw S3VectorsException.validation("segmentCount must be a positive integer.")
      if (segmentIndex !in 0..<segmentCount) {
        throw S3VectorsException.validation("segmentIndex must be between 0 and segmentCount - 1.")
      }
      all = all.filter { Math.floorMod(it.key.hashCode(), segmentCount) == segmentIndex }
    }

    if (nextToken != null) {
      val decoded = decodeToken(nextToken)
      all = all.dropWhile { it.key <= decoded }
    }

    val page = all.take(max)
    val resultToken = if (all.size > max) encodeToken(page.last().key) else null

    return ListVectorsResponse(
      vectors =
        page.map { stored ->
          ListOutputVector(
            key = stored.key,
            data = if (returnData && stored.floats != null) VectorData(stored.floats.map { it.toDouble() }) else null,
            metadata = if (returnMetadata) stored.metadata else null,
          )
        },
      nextToken = resultToken,
    )
  }

  fun deleteVectors(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    keys: List<String>,
  ) {
    val (bucketName, indexName) = vectorIndexService.resolveIndexId(bucketNameOrArn, indexNameOrArn)
    vectorIndexService.requireIndex(bucketName, indexName)
    keys.forEach { key -> vectorStore.deleteVector(bucketName, indexName, key) }
  }

  private fun validateVector(
    floats: FloatArray,
    dimension: Int,
    distanceMetric: String,
  ) {
    if (floats.size != dimension) throw S3VectorsException.dimensionMismatch(dimension, floats.size)
    if (floats.any { it.isNaN() || it.isInfinite() }) throw S3VectorsException.INVALID_VECTOR_DATA
    if (distanceMetric == "cosine" && floats.all { it == 0f }) throw S3VectorsException.ZERO_VECTOR_COSINE
  }
}
