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
import com.adobe.testing.s3mock.vectors.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.EncryptionConfiguration
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.vectors.dto.VectorBucket
import com.adobe.testing.s3mock.vectors.dto.VectorBucketSummary
import com.adobe.testing.s3mock.vectors.store.VectorBucketMetadata
import com.adobe.testing.s3mock.vectors.store.VectorBucketStore

open class VectorBucketService(
  private val vectorBucketStore: VectorBucketStore,
  private val region: String,
) : VectorServiceBase() {
  fun createVectorBucket(
    name: String,
    encryption: EncryptionConfiguration?,
    tags: Map<String, String>,
  ): CreateVectorBucketResponse {
    verifyBucketName(name)
    if (vectorBucketStore.doesBucketExist(name)) throw S3VectorsException.VECTOR_BUCKET_ALREADY_EXISTS
    vectorBucketStore.createVectorBucket(name, encryption?.sseType, encryption?.kmsKeyArn, tags)
    return CreateVectorBucketResponse(VectorArns.bucketArn(region, name))
  }

  fun getVectorBucket(nameOrArn: String): GetVectorBucketResponse {
    val name = resolveName(nameOrArn)
    val meta = requireBucket(name)
    return GetVectorBucketResponse(VectorBucket.from(meta, VectorArns.bucketArn(region, name)))
  }

  fun listVectorBuckets(
    maxResults: Int?,
    nextToken: String?,
    prefix: String?,
  ): ListVectorBucketsResponse {
    val max = (maxResults ?: 500).coerceIn(1, 500)
    val normalizedPrefix = prefix.orEmpty()

    var buckets =
      vectorBucketStore
        .listBuckets()
        .filter { it.name.startsWith(normalizedPrefix) }
        .sortedBy { it.name }

    if (nextToken != null) {
      val decoded = decodeToken(nextToken)
      buckets = buckets.dropWhile { it.name <= decoded }
    }

    val page = buckets.take(max)
    val resultToken = if (buckets.size > max) encodeToken(page.last().name) else null

    return ListVectorBucketsResponse(
      vectorBuckets = page.map { VectorBucketSummary.from(it, VectorArns.bucketArn(region, it.name)) },
      nextToken = resultToken,
    )
  }

  fun deleteVectorBucket(nameOrArn: String) {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    if (vectorBucketStore.hasIndexes(name)) throw S3VectorsException.VECTOR_BUCKET_NOT_EMPTY
    vectorBucketStore.deleteBucket(name)
  }

  fun requireBucket(name: String): VectorBucketMetadata {
    if (!vectorBucketStore.doesBucketExist(name)) throw S3VectorsException.VECTOR_BUCKET_NOT_FOUND
    return vectorBucketStore.getBucketMetadata(name)
  }

  fun putPolicy(
    nameOrArn: String,
    policy: String,
  ) {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    vectorBucketStore.storePolicy(name, policy)
  }

  fun getPolicy(nameOrArn: String): String? {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    return vectorBucketStore.getPolicy(name)
  }

  fun deletePolicy(nameOrArn: String) {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    vectorBucketStore.deletePolicy(name)
  }

  fun tagBucket(
    nameOrArn: String,
    tags: Map<String, String>,
  ) {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    vectorBucketStore.updateTags(name, tags)
  }

  fun getBucketTags(nameOrArn: String): Map<String, String> {
    val name = resolveName(nameOrArn)
    return requireBucket(name).tags
  }

  fun removeBucketTags(
    nameOrArn: String,
    tagKeys: List<String>,
  ) {
    val name = resolveName(nameOrArn)
    requireBucket(name)
    vectorBucketStore.removeTags(name, tagKeys)
  }

  fun resolveName(nameOrArn: String): String = if (VectorArns.isArn(nameOrArn)) VectorArns.bucketNameFromArn(nameOrArn) else nameOrArn

  private fun verifyBucketName(name: String) {
    if (name.length !in 3..63) throw S3VectorsException.INVALID_BUCKET_NAME
    if (!name.matches(Regex("[a-z0-9][a-z0-9\\-]*[a-z0-9]|[a-z0-9]"))) throw S3VectorsException.INVALID_BUCKET_NAME
  }
}
