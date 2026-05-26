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
import com.adobe.testing.s3mock.dto.CreateIndexResponse
import com.adobe.testing.s3mock.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.dto.GetIndexResponse
import com.adobe.testing.s3mock.dto.GetVectorBucketPolicyResponse
import com.adobe.testing.s3mock.dto.GetVectorBucketResponse
import com.adobe.testing.s3mock.dto.GetVectorsResponse
import com.adobe.testing.s3mock.dto.ListIndexesResponse
import com.adobe.testing.s3mock.dto.ListTagsForResourceResponse
import com.adobe.testing.s3mock.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.dto.ListVectorsResponse
import com.adobe.testing.s3mock.dto.OutputVector
import com.adobe.testing.s3mock.dto.PutInputVector
import com.adobe.testing.s3mock.dto.QueryOutputVector
import com.adobe.testing.s3mock.dto.QueryVectorsResponse
import com.adobe.testing.s3mock.dto.S3Metadata
import com.adobe.testing.s3mock.dto.S3Tags
import com.adobe.testing.s3mock.dto.VectorBucketDetail
import com.adobe.testing.s3mock.dto.VectorBucketSummary
import com.adobe.testing.s3mock.dto.VectorData
import com.adobe.testing.s3mock.dto.VectorEncryptionConfiguration
import com.adobe.testing.s3mock.dto.VectorIndexDetail
import com.adobe.testing.s3mock.dto.VectorIndexSummary
import com.adobe.testing.s3mock.dto.VectorMetadataConfiguration
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
    encryptionConfiguration: VectorEncryptionConfiguration?,
    tags: Map<String, String>,
  ): CreateVectorBucketResponse {
    val bucket = vectorStore.createVectorBucket(vectorBucketName, encryptionConfiguration, tags)
    return CreateVectorBucketResponse(vectorBucketArn = bucket.vectorBucketArn)
  }

  fun getVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): GetVectorBucketResponse =
    GetVectorBucketResponse(vectorBucket = VectorBucketDetail.from(vectorBucket(vectorBucketName, vectorBucketArn)!!))

  fun listVectorBuckets(
    prefix: String?,
    maxResults: Int?,
    nextToken: String?,
  ): ListVectorBucketsResponse {
    val listed = vectorStore.listVectorBuckets(prefix)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return ListVectorBucketsResponse(
      vectorBuckets = items.map(VectorBucketSummary::from),
      nextToken = token,
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
    metadataConfiguration: VectorMetadataConfiguration?,
    encryptionConfiguration: VectorEncryptionConfiguration?,
    tags: Map<String, String>,
  ): CreateIndexResponse {
    val validatedDataType = validateDataType(dataType)
    val validatedDistanceMetric = validateDistanceMetric(distanceMetric)
    val index =
      vectorStore.createIndex(
        vectorBucket(vectorBucketName, vectorBucketArn)!!,
        indexName,
        validatedDataType,
        dimension,
        validatedDistanceMetric,
        metadataConfiguration,
        encryptionConfiguration,
        tags,
      )
    return CreateIndexResponse(indexArn = index.indexArn)
  }

  fun getIndex(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ): GetIndexResponse = GetIndexResponse(index = VectorIndexDetail.from(index(vectorBucketName, indexName, indexArn)))

  fun listIndexes(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    maxResults: Int?,
    nextToken: String?,
    prefix: String?,
  ): ListIndexesResponse {
    val listed = vectorStore.listIndexes(vectorBucket(vectorBucketName, vectorBucketArn, false), prefix)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return ListIndexesResponse(
      indexes = items.map(VectorIndexSummary::from),
      nextToken = token,
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
    vectors: List<PutInputVector>,
  ) {
    val index = index(vectorBucketName, indexName, indexArn)
    val records =
      vectors.map {
        val key = it.key ?: throw VectorApiException.validation("Vector key is required.")
        val data = it.data?.float32 ?: throw VectorApiException.validation("Vector data (Float32) is required.")
        if (data.size != index.dimension) {
          throw VectorApiException.validation("Vector dimension does not match index dimension.")
        }
        VectorRecord(key = key, data = data, metadata = it.metadata?.values)
      }
    vectorStore.putVectors(index, records)
  }

  fun getVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    keys: List<String>,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): GetVectorsResponse {
    val index = index(vectorBucketName, indexName, indexArn)
    val vectors =
      vectorStore.getVectors(index, keys).map {
        toOutputVector(it, index.dataType, returnData, returnMetadata)
      }
    return GetVectorsResponse(vectors = vectors)
  }

  fun listVectors(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
    maxResults: Int?,
    nextToken: String?,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): ListVectorsResponse {
    val index = index(vectorBucketName, indexName, indexArn)
    val listed = vectorStore.listVectors(index)
    val (items, token) = paginate(listed, maxResults, nextToken)
    return ListVectorsResponse(
      vectors = items.map { toOutputVector(it, index.dataType, returnData, returnMetadata) },
      nextToken = token,
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
    queryVector: VectorData,
    returnMetadata: Boolean,
    returnDistance: Boolean,
  ): QueryVectorsResponse {
    val index = index(vectorBucketName, indexName, indexArn)
    val queryFloats = queryVector.float32 ?: throw VectorApiException.validation("QueryVector.Float32 is required.")
    if (queryFloats.size != index.dimension) {
      throw VectorApiException.validation("Query vector dimension does not match index dimension.")
    }
    val vectors =
      vectorStore
        .listVectors(index)
        .map { vector ->
          val distance = distance(index.distanceMetric, queryFloats, vector.data)
          vector to distance
        }.sortedBy { it.second }
        .take(topK)
        .map { (vector, distance) ->
          QueryOutputVector(
            key = vector.key,
            distance = if (returnDistance) distance else null,
            metadata = if (returnMetadata) S3Metadata(vector.metadata ?: emptyMap()) else null,
          )
        }
    return QueryVectorsResponse(
      vectors = vectors,
      distanceMetric = index.distanceMetric,
    )
  }

  fun putVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
    policy: String,
  ) {
    vectorStore.putVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn)!!, policy)
  }

  fun getVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): GetVectorBucketPolicyResponse {
    val policy = vectorStore.getVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn)!!)
    return GetVectorBucketPolicyResponse(policy = policy ?: "")
  }

  fun deleteVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    vectorStore.deleteVectorBucketPolicy(vectorBucket(vectorBucketName, vectorBucketArn)!!)
  }

  fun listTagsForResource(resourceArn: String): ListTagsForResourceResponse {
    val tags =
      runCatching { vectorStore.listTagsForResource(resourceArn) }
        .getOrElse { throw VectorApiException.notFound("Resource not found.") }
    return ListTagsForResourceResponse(tags = S3Tags(tags))
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

  private fun toOutputVector(
    vector: VectorRecord,
    dataType: String,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): OutputVector =
    OutputVector(
      key = vector.key,
      data = if (returnData) VectorData(float32 = vectorDataForType(dataType, vector.data)) else null,
      metadata = if (returnMetadata) S3Metadata(vector.metadata ?: emptyMap()) else null,
    )

  private fun vectorDataForType(
    dataType: String,
    values: List<Double>,
  ): List<Double> =
    when (dataType.uppercase()) {
      "FLOAT32" -> values
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
    private const val MAX_COSINE_DISTANCE = 2.0
  }
}
