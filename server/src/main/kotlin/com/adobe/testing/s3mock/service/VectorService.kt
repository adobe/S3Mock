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

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.CreateIndexRequest
import com.adobe.testing.s3mock.dto.CreateIndexResponse
import com.adobe.testing.s3mock.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.dto.GetIndexRequest
import com.adobe.testing.s3mock.dto.GetOutputVector
import com.adobe.testing.s3mock.dto.GetVectorBucketPolicyRequest
import com.adobe.testing.s3mock.dto.GetVectorBucketPolicyResponse
import com.adobe.testing.s3mock.dto.GetVectorBucketRequest
import com.adobe.testing.s3mock.dto.Index
import com.adobe.testing.s3mock.dto.IndexSummary
import com.adobe.testing.s3mock.dto.ListIndexesRequest
import com.adobe.testing.s3mock.dto.ListIndexesResponse
import com.adobe.testing.s3mock.dto.ListVectorBucketsRequest
import com.adobe.testing.s3mock.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.dto.ListVectorsRequest
import com.adobe.testing.s3mock.dto.ListVectorsResponse
import com.adobe.testing.s3mock.dto.QueryOutputVector
import com.adobe.testing.s3mock.dto.QueryVectorsRequest
import com.adobe.testing.s3mock.dto.QueryVectorsResponse
import com.adobe.testing.s3mock.dto.VectorBucket
import com.adobe.testing.s3mock.dto.VectorBucketSummary
import com.adobe.testing.s3mock.store.VectorStore

class VectorService(
  private val vectorStore: VectorStore,
) {
  fun createVectorBucket(request: CreateVectorBucketRequest): CreateVectorBucketResponse {
    val bucketName = requireValue(request.vectorBucketName, "vectorBucketName")
    ensure(!vectorStore.doesVectorBucketExist(bucketName), S3Exception.VECTOR_CONFLICT)

    val metadata = vectorStore.createVectorBucket(bucketName, request.encryptionConfiguration, request.tags.orEmpty())
    return CreateVectorBucketResponse(metadata.vectorBucketArn)
  }

  fun deleteVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    val bucketName = resolveBucketName(vectorBucketName, vectorBucketArn)
    ensure(vectorStore.doesVectorBucketExist(bucketName), S3Exception.VECTOR_NOT_FOUND)
    ensure(vectorStore.deleteVectorBucket(bucketName), S3Exception.VECTOR_CONFLICT)
  }

  fun getVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): VectorBucket {
    val bucketName = resolveBucketName(vectorBucketName, vectorBucketArn)
    ensure(vectorStore.doesVectorBucketExist(bucketName), S3Exception.VECTOR_NOT_FOUND)
    val metadata = vectorStore.getVectorBucket(bucketName)

    return VectorBucket(
      creationTime = metadata.creationTime,
      encryptionConfiguration = metadata.encryptionConfiguration,
      vectorBucketArn = metadata.vectorBucketArn,
      vectorBucketName = metadata.vectorBucketName,
    )
  }

  fun listVectorBuckets(request: ListVectorBucketsRequest): ListVectorBucketsResponse {
    val maxResults = request.maxResults ?: DEFAULT_MAX_RESULTS
    val (buckets, nextToken) =
      vectorStore.listVectorBuckets(
        prefix = request.prefix,
        maxResults = maxResults,
        nextToken = request.nextToken,
      )

    return ListVectorBucketsResponse(
      nextToken = nextToken,
      vectorBuckets =
        buckets.map {
          VectorBucketSummary(
            creationTime = it.creationTime,
            vectorBucketArn = it.vectorBucketArn,
            vectorBucketName = it.vectorBucketName,
          )
        },
    )
  }

  fun createIndex(request: CreateIndexRequest): CreateIndexResponse {
    val bucketName = resolveBucketName(request.vectorBucketName, request.vectorBucketArn)
    ensure(vectorStore.doesVectorBucketExist(bucketName), S3Exception.VECTOR_NOT_FOUND)
    val indexName = requireValue(request.indexName, "indexName")
    val dataType = requireValue(request.dataType, "dataType")
    val dimension = requireValue(request.dimension, "dimension")
    val distanceMetric = requireValue(request.distanceMetric, "distanceMetric")
    ensure(dataType == DATA_TYPE_FLOAT32, S3Exception.VECTOR_VALIDATION)
    ensure(distanceMetric in setOf(DISTANCE_COSINE, DISTANCE_EUCLIDEAN), S3Exception.VECTOR_VALIDATION)
    ensure(dimension > 0, S3Exception.VECTOR_VALIDATION)

    val metadata =
      vectorStore.createIndex(
        vectorBucketName = bucketName,
        indexName = indexName,
        dataType = dataType,
        dimension = dimension,
        distanceMetric = distanceMetric,
        encryptionConfiguration = request.encryptionConfiguration,
        metadataConfiguration = request.metadataConfiguration,
        tags = request.tags.orEmpty(),
      )

    return CreateIndexResponse(metadata.indexArn)
  }

  fun deleteIndex(
    indexName: String?,
    vectorBucketName: String?,
    indexArn: String?,
  ) {
    val resolved = resolveIndex(indexName, vectorBucketName, indexArn)
    ensure(vectorStore.deleteIndex(resolved.first, resolved.second), S3Exception.VECTOR_CONFLICT)
  }

  fun getIndex(request: GetIndexRequest): Index {
    val resolved = resolveIndex(request.indexName, request.vectorBucketName, request.indexArn)
    val metadata = vectorStore.getIndex(resolved.first, resolved.second)
    return Index(
      creationTime = metadata.creationTime,
      dataType = metadata.dataType,
      dimension = metadata.dimension,
      distanceMetric = metadata.distanceMetric,
      encryptionConfiguration = metadata.encryptionConfiguration,
      indexArn = metadata.indexArn,
      indexName = metadata.indexName,
      metadataConfiguration = metadata.metadataConfiguration,
      vectorBucketName = metadata.vectorBucketName,
    )
  }

  fun listIndexes(request: ListIndexesRequest): ListIndexesResponse {
    val bucketName = resolveBucketName(request.vectorBucketName, request.vectorBucketArn)
    val maxResults = request.maxResults ?: DEFAULT_MAX_RESULTS
    val (indexes, nextToken) =
      vectorStore.listIndexes(
        vectorBucketName = bucketName,
        prefix = request.prefix,
        maxResults = maxResults,
        nextToken = request.nextToken,
      )

    return ListIndexesResponse(
      indexes =
        indexes.map {
          IndexSummary(
            creationTime = it.creationTime,
            indexArn = it.indexArn,
            indexName = it.indexName,
            vectorBucketName = it.vectorBucketName,
          )
        },
      nextToken = nextToken,
    )
  }

  fun putVectors(
    indexName: String?,
    vectorBucketName: String?,
    indexArn: String?,
    vectors: List<com.adobe.testing.s3mock.dto.PutInputVector>?,
  ) {
    val inputVectors = requireValue(vectors, "vectors")
    val resolved = resolveIndex(indexName, vectorBucketName, indexArn)
    val indexMetadata = vectorStore.getIndex(resolved.first, resolved.second)

    inputVectors.forEach {
      ensure(it.data.float32?.size == indexMetadata.dimension, S3Exception.VECTOR_VALIDATION)
    }

    vectorStore.putVectors(resolved.first, resolved.second, inputVectors)
  }

  fun getVectors(
    indexName: String?,
    vectorBucketName: String?,
    indexArn: String?,
    keys: List<String>?,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): List<GetOutputVector> {
    val resolved = resolveIndex(indexName, vectorBucketName, indexArn)
    val requestedKeys = requireValue(keys, "keys")
    return vectorStore
      .getVectors(resolved.first, resolved.second, requestedKeys)
      .map {
        GetOutputVector(
          data = if (returnData) it.data else null,
          key = it.key,
          metadata = if (returnMetadata) it.metadata else null,
        )
      }
  }

  fun listVectors(request: ListVectorsRequest): ListVectorsResponse {
    val resolved = resolveIndex(request.indexName, request.vectorBucketName, request.indexArn)
    val maxResults = request.maxResults ?: DEFAULT_MAX_RESULTS
    val (vectors, nextToken) =
      vectorStore.listVectors(
        vectorBucketName = resolved.first,
        indexName = resolved.second,
        maxResults = maxResults,
        nextToken = request.nextToken,
        segmentCount = request.segmentCount,
        segmentIndex = request.segmentIndex,
      )

    return ListVectorsResponse(
      nextToken = nextToken,
      vectors =
        vectors.map {
          GetOutputVector(
            data = if (request.returnData == true) it.data else null,
            key = it.key,
            metadata = if (request.returnMetadata == true) it.metadata else null,
          )
        },
    )
  }

  fun deleteVectors(
    indexName: String?,
    vectorBucketName: String?,
    indexArn: String?,
    keys: List<String>?,
  ) {
    val resolved = resolveIndex(indexName, vectorBucketName, indexArn)
    val requestedKeys = requireValue(keys, "keys")
    vectorStore.deleteVectors(resolved.first, resolved.second, requestedKeys)
  }

  fun queryVectors(request: QueryVectorsRequest): QueryVectorsResponse {
    val resolved = resolveIndex(request.indexName, request.vectorBucketName, request.indexArn)
    val topK = requireValue(request.topK, "topK")
    val queryVector = requireValue(request.queryVector, "queryVector")
    val indexMetadata = vectorStore.getIndex(resolved.first, resolved.second)
    ensure(queryVector.float32?.size == indexMetadata.dimension, S3Exception.VECTOR_VALIDATION)

    val vectors =
      vectorStore.queryVectors(resolved.first, resolved.second, queryVector, topK).map {
        QueryOutputVector(
          distance = if (request.returnDistance == true) it.second else null,
          key = it.first.key,
          metadata = if (request.returnMetadata == true) it.first.metadata else null,
        )
      }

    return QueryVectorsResponse(distanceMetric = indexMetadata.distanceMetric, vectors = vectors)
  }

  fun putVectorBucketPolicy(
    policy: String?,
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    val bucketName = resolveBucketName(vectorBucketName, vectorBucketArn)
    vectorStore.putVectorBucketPolicy(bucketName, requireValue(policy, "policy"))
  }

  fun getVectorBucketPolicy(request: GetVectorBucketPolicyRequest): GetVectorBucketPolicyResponse {
    val bucketName = resolveBucketName(request.vectorBucketName, request.vectorBucketArn)
    val policy = vectorStore.getVectorBucketPolicy(bucketName)
    ensure(policy != null, S3Exception.VECTOR_NOT_FOUND)
    return GetVectorBucketPolicyResponse(policy)
  }

  fun deleteVectorBucketPolicy(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ) {
    val bucketName = resolveBucketName(vectorBucketName, vectorBucketArn)
    ensure(vectorStore.getVectorBucketPolicy(bucketName) != null, S3Exception.VECTOR_NOT_FOUND)
    vectorStore.deleteVectorBucketPolicy(bucketName)
  }

  fun tagResource(
    resourceArn: String,
    tags: Map<String, String>,
  ) {
    if (resourceArn.contains("/index/")) {
      val resolved = resolveIndex(null, null, resourceArn)
      vectorStore.putIndexTags(resolved.first, resolved.second, tags)
      return
    }

    val bucketName = resolveBucketName(null, resourceArn)
    vectorStore.putVectorBucketTags(bucketName, tags)
  }

  fun untagResource(
    resourceArn: String,
    tagKeys: List<String>,
  ) {
    vectorStore.deleteTags(resourceArn, tagKeys)
  }

  fun listTagsForResource(resourceArn: String): Map<String, String> = vectorStore.listTags(resourceArn)

  private fun resolveBucketName(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): String {
    if (!vectorBucketName.isNullOrBlank()) {
      return vectorBucketName
    }
    if (!vectorBucketArn.isNullOrBlank()) {
      return try {
        vectorStore.getVectorBucketByArn(vectorBucketArn).vectorBucketName
      } catch (_: IllegalArgumentException) {
        throw S3Exception.VECTOR_VALIDATION
      } catch (_: IllegalStateException) {
        throw S3Exception.VECTOR_NOT_FOUND
      }
    }
    throw S3Exception.VECTOR_VALIDATION
  }

  private fun resolveIndex(
    indexName: String?,
    vectorBucketName: String?,
    indexArn: String?,
  ): Pair<String, String> {
    if (!indexArn.isNullOrBlank()) {
      val index =
        try {
          vectorStore.getIndexByArn(indexArn)
        } catch (_: IllegalArgumentException) {
          throw S3Exception.VECTOR_VALIDATION
        } catch (_: IllegalStateException) {
          throw S3Exception.VECTOR_NOT_FOUND
        }
      return index.vectorBucketName to index.indexName
    }

    if (!vectorBucketName.isNullOrBlank() && !indexName.isNullOrBlank()) {
      try {
        vectorStore.getIndex(vectorBucketName, indexName)
      } catch (_: IllegalStateException) {
        throw S3Exception.VECTOR_NOT_FOUND
      }
      return vectorBucketName to indexName
    }

    throw S3Exception.VECTOR_VALIDATION
  }

  private fun ensure(
    condition: Boolean,
    exception: S3Exception,
  ) {
    if (!condition) {
      throw exception
    }
  }

  private fun <T> requireValue(
    value: T?,
    fieldName: String,
  ): T = value ?: throw S3Exception.vectorValidationField(fieldName)

  companion object {
    private const val DATA_TYPE_FLOAT32 = "float32"
    private const val DISTANCE_COSINE = "cosine"
    private const val DISTANCE_EUCLIDEAN = "euclidean"
    private const val DEFAULT_MAX_RESULTS = 1000
  }
}
