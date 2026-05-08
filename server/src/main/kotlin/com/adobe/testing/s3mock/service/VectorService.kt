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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.controller.VectorApiException
import com.adobe.testing.s3mock.store.VectorBucketRecord
import com.adobe.testing.s3mock.store.VectorIndexRecord
import com.adobe.testing.s3mock.store.VectorRecord
import com.adobe.testing.s3mock.store.VectorStore
import kotlin.math.sqrt

class VectorService(
  private val vectorStore: VectorStore,
) {
  fun createVectorBucket(
    vectorBucketName: String,
    encryptionConfiguration: Map<String, Any?>?,
    tags: Map<String, String>,
  ): Map<String, Any?> {
    val bucket = vectorStore.createVectorBucket(vectorBucketName, encryptionConfiguration, tags)
    return mapOf("VectorBucketArn" to bucket.vectorBucketArn)
  }

  fun getVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): Map<String, Any?> = mapOf("VectorBucket" to toVectorBucket(vectorBucket(vectorBucketName, vectorBucketArn)))

  fun listVectorBuckets(
    prefix: String?,
    maxResults: Int?,
    nextToken: String?,
  ): Map<String, Any?> {
    val listed = vectorStore.listVectorBuckets(prefix)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return mapOf(
      "VectorBuckets" to items.map(::toVectorBucketSummary),
      "NextToken" to token,
    )
  }

  fun deleteVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    val deleted = vectorStore.deleteVectorBucket(vectorBucketName, vectorBucketArn)
    if (!deleted) throw VectorApiException.notFound("Vector bucket not found.")
  }

  fun createIndex(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    indexName: String,
    dataType: String,
    dimension: Int,
    distanceMetric: String,
    metadataConfiguration: Map<String, Any?>?,
    encryptionConfiguration: Map<String, Any?>?,
    tags: Map<String, String>,
  ): Map<String, Any?> {
    val validatedDataType = validateDataType(dataType)
    val validatedDistanceMetric = validateDistanceMetric(distanceMetric)
    val index =
      vectorStore.createIndex(
        vectorBucket(vectorBucketName, vectorBucketArn),
        indexName,
        validatedDataType,
        dimension,
        validatedDistanceMetric,
        metadataConfiguration,
        encryptionConfiguration,
        tags,
      )
    return mapOf("IndexArn" to index.indexArn)
  }

  fun getIndex(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ): Map<String, Any?> = mapOf("Index" to toIndex(index(vectorBucketName, indexName, indexArn)))

  fun listIndexes(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    maxResults: Int?,
    nextToken: String?,
    prefix: String?,
  ): Map<String, Any?> {
    val listed = vectorStore.listIndexes(vectorBucket(vectorBucketName, vectorBucketArn, false), prefix)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return mapOf(
      "Indexes" to items.map(::toIndexSummary),
      "NextToken" to token,
    )
  }

  fun deleteIndex(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ) {
    val deleted = vectorStore.deleteIndex(vectorBucketName, indexName, indexArn)
    if (!deleted) throw VectorApiException.notFound("Index not found.")
  }

  fun putVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    vectors: List<VectorRecord>,
  ) {
    val index = index(vectorBucketName, indexName, indexArn)
    vectors.forEach {
      if (it.data.size != index.dimension) {
        throw VectorApiException.validation("Vector dimension does not match index dimension.")
      }
    }
    vectorStore.putVectors(index, vectors)
  }

  fun getVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    keys: List<String>,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): Map<String, Any?> {
    val index = index(vectorBucketName, indexName, indexArn)
    val vectors =
      vectorStore.getVectors(index, keys).map {
        toOutputVector(
          it,
          index.dataType,
          returnData,
          returnMetadata,
        )
      }
    return mapOf("Vectors" to vectors)
  }

  fun listVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    maxResults: Int?,
    nextToken: String?,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): Map<String, Any?> {
    val index = index(vectorBucketName, indexName, indexArn)
    val listed = vectorStore.listVectors(index)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return mapOf(
      "Vectors" to items.map { toOutputVector(it, index.dataType, returnData, returnMetadata) },
      "NextToken" to token,
    )
  }

  fun deleteVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    keys: List<String>,
  ) {
    vectorStore.deleteVectors(index(vectorBucketName, indexName, indexArn), keys)
  }

  fun queryVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    topK: Int,
    queryVector: List<Double>,
    returnMetadata: Boolean,
    returnDistance: Boolean,
  ): Map<String, Any?> {
    val index = index(vectorBucketName, indexName, indexArn)
    if (queryVector.size != index.dimension) {
      throw VectorApiException.validation("Query vector dimension does not match index dimension.")
    }
    val vectors =
      vectorStore
        .listVectors(index)
        .map { vector ->
          val distance = distance(index.distanceMetric, queryVector, vector.data)
          vector to distance
        }.sortedBy { it.second }
        .take(topK)
        .map { (vector, distance) ->
          buildMap<String, Any?> {
            put("Key", vector.key)
            if (returnMetadata) put("Metadata", vector.metadata ?: emptyMap<String, String>())
            if (returnDistance) put("Distance", distance)
          }
        }
    return mapOf(
      "Vectors" to vectors,
      "DistanceMetric" to index.distanceMetric,
    )
  }

  fun putVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    policy: String,
  ) {
    vectorStore.putVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn), policy)
  }

  fun getVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): Map<String, Any?> {
    val policy = vectorStore.getVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn))
    return mapOf("Policy" to (policy ?: ""))
  }

  fun deleteVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    vectorStore.deleteVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn))
  }

  fun listTagsForResource(resourceArn: String): Map<String, Any?> {
    val tags =
      runCatching { vectorStore.listTagsForResource(resourceArn) }
        .getOrElse { throw VectorApiException.notFound("Resource not found.") }
    return mapOf("Tags" to tags)
  }

  fun tagResource(
    resourceArn: String,
    tags: Map<String, String>,
  ) {
    runCatching { vectorStore.tagResource(resourceArn, tags) }
      .getOrElse { throw VectorApiException.notFound("Resource not found.") }
  }

  fun untagResource(
    resourceArn: String,
    tagKeys: List<String>,
  ) {
    runCatching { vectorStore.untagResource(resourceArn, tagKeys) }
      .getOrElse { throw VectorApiException.notFound("Resource not found.") }
  }

  private fun toVectorBucket(bucket: VectorBucketRecord): Map<String, Any?> =
    buildMap {
      put("VectorBucketName", bucket.vectorBucketName)
      put("VectorBucketArn", bucket.vectorBucketArn)
      put("CreationTime", bucket.creationTime.toString())
      put("EncryptionConfiguration", bucket.encryptionConfiguration)
    }

  private fun toVectorBucketSummary(bucket: VectorBucketRecord): Map<String, Any?> =
    mapOf(
      "VectorBucketName" to bucket.vectorBucketName,
      "VectorBucketArn" to bucket.vectorBucketArn,
      "CreationTime" to bucket.creationTime.toString(),
    )

  private fun toIndex(index: VectorIndexRecord): Map<String, Any?> =
    buildMap {
      put("VectorBucketName", index.vectorBucketName)
      put("IndexName", index.indexName)
      put("IndexArn", index.indexArn)
      put("CreationTime", index.creationTime.toString())
      put("DataType", index.dataType)
      put("Dimension", index.dimension)
      put("DistanceMetric", index.distanceMetric)
      put("MetadataConfiguration", index.metadataConfiguration)
      put("EncryptionConfiguration", index.encryptionConfiguration)
    }

  private fun toIndexSummary(index: VectorIndexRecord): Map<String, Any?> =
    mapOf(
      "VectorBucketName" to index.vectorBucketName,
      "IndexName" to index.indexName,
      "IndexArn" to index.indexArn,
      "CreationTime" to index.creationTime.toString(),
    )

  private fun toOutputVector(
    vector: VectorRecord,
    dataType: String,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): Map<String, Any?> =
    buildMap {
      put("Key", vector.key)
      if (returnData) put("Data", mapOf(dataFieldName(dataType) to vector.data))
      if (returnMetadata) put("Metadata", vector.metadata ?: emptyMap<String, String>())
    }

  private fun dataFieldName(dataType: String): String =
    when (dataType.uppercase()) {
      "FLOAT32" -> "Float32"
      else -> throw VectorApiException.validation("Unsupported data type: $dataType")
    }

  private fun distance(
    distanceMetric: String,
    query: List<Double>,
    candidate: List<Double>,
  ): Double =
    when (validateDistanceMetric(distanceMetric)) {
      "COSINE" -> cosineDistance(query, candidate)
      "EUCLIDEAN" -> euclideanDistance(query, candidate)
      else -> throw VectorApiException.validation("Unsupported distance metric: $distanceMetric")
    }

  private fun euclideanDistance(
    lhs: List<Double>,
    rhs: List<Double>,
  ): Double = sqrt(lhs.zip(rhs).sumOf { (a, b) -> (a - b) * (a - b) })

  /**
   * Returns cosine distance in range [0, 2], where 0 means identical direction and 2 opposite.
   * For zero-magnitude vectors, returns [MAX_COSINE_DISTANCE].
   */
  private fun cosineDistance(
    lhs: List<Double>,
    rhs: List<Double>,
  ): Double {
    val dot = lhs.zip(rhs).sumOf { (a, b) -> a * b }
    val lhsMagnitude = sqrt(lhs.sumOf { it * it })
    val rhsMagnitude = sqrt(rhs.sumOf { it * it })
    if (lhsMagnitude == 0.0 || rhsMagnitude == 0.0) return MAX_COSINE_DISTANCE
    return 1.0 - (dot / (lhsMagnitude * rhsMagnitude))
  }

  private fun validateDataType(dataType: String): String =
    when (dataType.uppercase()) {
      "FLOAT32" -> "FLOAT32"
      else -> throw VectorApiException.validation("Unsupported data type: $dataType")
    }

  private fun validateDistanceMetric(distanceMetric: String): String =
    when (distanceMetric.uppercase()) {
      "EUCLIDEAN" -> "EUCLIDEAN"
      "COSINE" -> "COSINE"
      else -> throw VectorApiException.validation("Unsupported distance metric: $distanceMetric")
    }

  private fun vectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    required: Boolean = true,
  ): VectorBucketRecord? {
    val bucket = vectorStore.getVectorBucket(vectorBucketName, vectorBucketArn)
    if (bucket == null && required) throw VectorApiException.notFound("Vector bucket not found.")
    return bucket
  }

  private fun index(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ): VectorIndexRecord =
    vectorStore.getIndex(vectorBucketName, indexName, indexArn) ?: throw VectorApiException.notFound("Index not found.")

  /**
   * Applies token-based pagination with S3 Vectors-compatible defaults:
   * default page size is 100 and the maximum allowed page size is 1000.
   */
  private fun <T> paginate(
    values: List<T>,
    maxResults: Int?,
    nextToken: String?,
  ): Pair<List<T>, String?> {
    val limit = (maxResults ?: 100).coerceIn(1, 1000)
    val offset =
      nextToken
        ?.toIntOrNull()
        ?.coerceAtLeast(0)
        ?: 0
    val end = (offset + limit).coerceAtMost(values.size)
    val items = if (offset >= values.size) emptyList() else values.subList(offset, end)
    val token = if (end < values.size) end.toString() else null
    return items to token
  }

  companion object {
    private const val MAX_COSINE_DISTANCE = 1.0
  }
}
