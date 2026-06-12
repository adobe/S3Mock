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
package com.adobe.testing.s3mock.dto

import com.fasterxml.jackson.annotation.JsonInclude
import tools.jackson.databind.JsonNode

@JsonInclude(JsonInclude.Include.NON_NULL)
data class EncryptionConfiguration(
  val kmsKeyArn: String? = null,
  val sseType: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MetadataConfiguration(
  val nonFilterableMetadataKeys: List<String>? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class VectorData(
  val float32: List<Float>? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class PutInputVector(
  val data: VectorData,
  val key: String,
  val metadata: JsonNode? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetOutputVector(
  val data: VectorData? = null,
  val key: String,
  val metadata: JsonNode? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class QueryOutputVector(
  val distance: Double? = null,
  val key: String,
  val metadata: JsonNode? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class VectorBucket(
  val creationTime: Long,
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val vectorBucketArn: String,
  val vectorBucketName: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class VectorBucketSummary(
  val creationTime: Long,
  val vectorBucketArn: String,
  val vectorBucketName: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class Index(
  val creationTime: Long,
  val dataType: String,
  val dimension: Int,
  val distanceMetric: String,
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val indexArn: String,
  val indexName: String,
  val metadataConfiguration: MetadataConfiguration? = null,
  val vectorBucketName: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class IndexSummary(
  val creationTime: Long,
  val indexArn: String,
  val indexName: String,
  val vectorBucketName: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ValidationExceptionField(
  val message: String,
  val path: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ValidationErrorResponse(
  val fieldList: List<ValidationExceptionField>? = null,
  val message: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateVectorBucketRequest(
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val tags: Map<String, String>? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateVectorBucketResponse(
  val vectorBucketArn: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DeleteVectorBucketRequest(
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorBucketRequest(
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorBucketResponse(
  val vectorBucket: VectorBucket,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListVectorBucketsRequest(
  val maxResults: Int? = null,
  val nextToken: String? = null,
  val prefix: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListVectorBucketsResponse(
  val nextToken: String? = null,
  val vectorBuckets: List<VectorBucketSummary>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateIndexRequest(
  val dataType: String? = null,
  val dimension: Int? = null,
  val distanceMetric: String? = null,
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val indexName: String? = null,
  val metadataConfiguration: MetadataConfiguration? = null,
  val tags: Map<String, String>? = null,
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateIndexResponse(
  val indexArn: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DeleteIndexRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetIndexRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetIndexResponse(
  val index: Index,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListIndexesRequest(
  val maxResults: Int? = null,
  val nextToken: String? = null,
  val prefix: String? = null,
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListIndexesResponse(
  val indexes: List<IndexSummary>,
  val nextToken: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class PutVectorsRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val vectorBucketName: String? = null,
  val vectors: List<PutInputVector>? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorsRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val keys: List<String>? = null,
  val returnData: Boolean? = null,
  val returnMetadata: Boolean? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorsResponse(
  val vectors: List<GetOutputVector>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DeleteVectorsRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val keys: List<String>? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListVectorsRequest(
  val indexArn: String? = null,
  val indexName: String? = null,
  val maxResults: Int? = null,
  val nextToken: String? = null,
  val returnData: Boolean? = null,
  val returnMetadata: Boolean? = null,
  val segmentCount: Int? = null,
  val segmentIndex: Int? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListVectorsResponse(
  val nextToken: String? = null,
  val vectors: List<GetOutputVector>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class QueryVectorsRequest(
  val filter: JsonNode? = null,
  val indexArn: String? = null,
  val indexName: String? = null,
  val queryVector: VectorData? = null,
  val returnDistance: Boolean? = null,
  val returnMetadata: Boolean? = null,
  val topK: Int? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class QueryVectorsResponse(
  val distanceMetric: String,
  val vectors: List<QueryOutputVector>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class PutVectorBucketPolicyRequest(
  val policy: String? = null,
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorBucketPolicyRequest(
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class GetVectorBucketPolicyResponse(
  val policy: String,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DeleteVectorBucketPolicyRequest(
  val vectorBucketArn: String? = null,
  val vectorBucketName: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class TagResourceRequest(
  val tags: Map<String, String>? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ListTagsForResourceResponse(
  val tags: Map<String, String>,
)
