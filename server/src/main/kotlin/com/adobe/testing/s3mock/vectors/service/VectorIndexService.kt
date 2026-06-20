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
import com.adobe.testing.s3mock.vectors.dto.CreateIndexResponse
import com.adobe.testing.s3mock.vectors.dto.EncryptionConfiguration
import com.adobe.testing.s3mock.vectors.dto.GetIndexResponse
import com.adobe.testing.s3mock.vectors.dto.IndexSummary
import com.adobe.testing.s3mock.vectors.dto.ListIndexesResponse
import com.adobe.testing.s3mock.vectors.dto.MetadataConfiguration
import com.adobe.testing.s3mock.vectors.dto.VectorIndex
import com.adobe.testing.s3mock.vectors.store.VectorIndexMetadata
import com.adobe.testing.s3mock.vectors.store.VectorIndexStore

open class VectorIndexService(
  private val vectorBucketService: VectorBucketService,
  private val vectorIndexStore: VectorIndexStore,
  private val region: String,
) : VectorServiceBase() {
  fun createIndex(
    bucketNameOrArn: String,
    indexName: String,
    dataType: String,
    dimension: Int,
    distanceMetric: String,
    metadataConfiguration: MetadataConfiguration?,
    encryption: EncryptionConfiguration?,
    tags: Map<String, String>,
  ): CreateIndexResponse {
    verifyIndexName(indexName)
    if (dimension !in 1..4096) throw S3VectorsException.INVALID_DIMENSION
    if (dataType != "float32") throw S3VectorsException.validation("Unsupported dataType: $dataType. Supported values: float32")
    if (distanceMetric !in setOf("euclidean", "cosine")) {
      throw S3VectorsException.validation("Unsupported distanceMetric: $distanceMetric. Supported values: euclidean, cosine")
    }
    val bucketName = vectorBucketService.resolveName(bucketNameOrArn)
    vectorBucketService.requireBucket(bucketName)
    if (vectorIndexStore.doesIndexExist(bucketName, indexName)) throw S3VectorsException.INDEX_ALREADY_EXISTS

    vectorIndexStore.createIndex(
      bucketName = bucketName,
      indexName = indexName,
      dataType = dataType,
      dimension = dimension,
      distanceMetric = distanceMetric,
      sseType = encryption?.sseType,
      kmsKeyArn = encryption?.kmsKeyArn,
      nonFilterableMetadataKeys = metadataConfiguration?.nonFilterableMetadataKeys.orEmpty(),
      tags = tags,
    )
    return CreateIndexResponse(VectorArns.indexArn(region, bucketName, indexName))
  }

  fun getIndex(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
  ): GetIndexResponse {
    val (bucketName, indexName) = resolveIndexId(bucketNameOrArn, indexNameOrArn)
    val meta = requireIndex(bucketName, indexName)
    return GetIndexResponse(VectorIndex.from(meta, VectorArns.indexArn(region, bucketName, indexName)))
  }

  fun listIndexes(
    bucketNameOrArn: String,
    prefix: String?,
    maxResults: Int?,
    nextToken: String?,
  ): ListIndexesResponse {
    val max = (maxResults ?: 500).coerceIn(1, 500)
    val bucketName = vectorBucketService.resolveName(bucketNameOrArn)
    vectorBucketService.requireBucket(bucketName)
    val normalizedPrefix = prefix.orEmpty()

    var indexes =
      vectorIndexStore
        .listIndexes(bucketName)
        .filter { it.name.startsWith(normalizedPrefix) }
        .sortedBy { it.name }

    if (nextToken != null) {
      val decoded = decodeToken(nextToken)
      indexes = indexes.dropWhile { it.name <= decoded }
    }

    val page = indexes.take(max)
    val resultToken = if (indexes.size > max) encodeToken(page.last().name) else null

    return ListIndexesResponse(
      indexes = page.map { IndexSummary.from(it, VectorArns.indexArn(region, bucketName, it.name)) },
      nextToken = resultToken,
    )
  }

  fun deleteIndex(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
  ) {
    val (bucketName, indexName) = resolveIndexId(bucketNameOrArn, indexNameOrArn)
    requireIndex(bucketName, indexName)
    vectorIndexStore.deleteIndex(bucketName, indexName)
  }

  fun requireIndex(
    bucketName: String,
    indexName: String,
  ): VectorIndexMetadata {
    if (!vectorIndexStore.doesIndexExist(bucketName, indexName)) throw S3VectorsException.INDEX_NOT_FOUND
    return vectorIndexStore.getIndexMetadata(bucketName, indexName)
  }

  fun tagIndex(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    tags: Map<String, String>,
  ) {
    val (bucketName, indexName) = resolveIndexId(bucketNameOrArn, indexNameOrArn)
    requireIndex(bucketName, indexName)
    vectorIndexStore.updateTags(bucketName, indexName, tags)
  }

  fun getIndexTags(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
  ): Map<String, String> {
    val (bucketName, indexName) = resolveIndexId(bucketNameOrArn, indexNameOrArn)
    return requireIndex(bucketName, indexName).tags
  }

  fun removeIndexTags(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    tagKeys: List<String>,
  ) {
    val (bucketName, indexName) = resolveIndexId(bucketNameOrArn, indexNameOrArn)
    requireIndex(bucketName, indexName)
    vectorIndexStore.removeTags(bucketName, indexName, tagKeys)
  }

  /**
   * Resolves (bucketName, indexName) from either an indexArn or a (bucketNameOrArn, indexName) pair.
   */
  fun resolveIndexId(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
  ): Pair<String, String> =
    if (VectorArns.isArn(indexNameOrArn)) {
      VectorArns.bucketNameFromArn(indexNameOrArn) to VectorArns.indexNameFromArn(indexNameOrArn)
    } else {
      val bucket =
        bucketNameOrArn?.let { vectorBucketService.resolveName(it) }
          ?: throw S3VectorsException.validation("vectorBucketName is required when indexArn is not provided.")
      bucket to indexNameOrArn
    }

  private fun verifyIndexName(name: String) {
    if (name.length !in 3..63) throw S3VectorsException.INVALID_INDEX_NAME
    if (!name.matches(Regex("[a-z0-9][a-z0-9\\-]*[a-z0-9]|[a-z0-9]"))) throw S3VectorsException.INVALID_INDEX_NAME
  }
}
