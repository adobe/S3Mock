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
package com.adobe.testing.s3mock.store

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class VectorStore(
  private val region: String,
) {
  private val bucketsByName: MutableMap<String, VectorBucketRecord> = ConcurrentHashMap()
  private val bucketsByArn: MutableMap<String, VectorBucketRecord> = ConcurrentHashMap()
  private val indexesByArn: MutableMap<String, VectorIndexRecord> = ConcurrentHashMap()

  @Synchronized
  fun createVectorBucket(
    vectorBucketName: String,
    encryptionConfiguration: Map<String, Any?>?,
    tags: Map<String, String>,
  ): VectorBucketRecord {
    val existing = bucketsByName[vectorBucketName]
    if (existing != null) return existing

    val arn = vectorBucketArn(vectorBucketName)
    val created =
      VectorBucketRecord(
        vectorBucketName = vectorBucketName,
        vectorBucketArn = arn,
        creationTime = Instant.now(),
        encryptionConfiguration = encryptionConfiguration,
        tags = tags.toMutableMap(),
      )
    bucketsByName[vectorBucketName] = created
    bucketsByArn[arn] = created
    return created
  }

  fun listVectorBuckets(prefix: String?): List<VectorBucketRecord> =
    bucketsByName.values
      .asSequence()
      .filter { prefix.isNullOrBlank() || it.vectorBucketName.startsWith(prefix) }
      .sortedBy { it.vectorBucketName }
      .toList()

  fun getVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): VectorBucketRecord? = vectorBucketName?.let { bucketsByName[it] } ?: vectorBucketArn?.let { bucketsByArn[it] }

  @Synchronized
  fun deleteVectorBucket(
    vectorBucketName: String?,
    vectorBucketArn: String?,
  ): Boolean {
    val bucket = getVectorBucket(vectorBucketName, vectorBucketArn) ?: return false
    if (bucket.indexes.isNotEmpty()) return false
    bucketsByName.remove(bucket.vectorBucketName)
    bucketsByArn.remove(bucket.vectorBucketArn)
    return true
  }

  @Synchronized
  fun createIndex(
    bucket: VectorBucketRecord,
    indexName: String,
    dataType: String,
    dimension: Int,
    distanceMetric: String,
    metadataConfiguration: Map<String, Any?>?,
    encryptionConfiguration: Map<String, Any?>?,
    tags: Map<String, String>,
  ): VectorIndexRecord {
    val existing = bucket.indexes[indexName]
    if (existing != null) return existing

    val arn = indexArn(bucket.vectorBucketName, indexName)
    val created =
      VectorIndexRecord(
        vectorBucketName = bucket.vectorBucketName,
        indexName = indexName,
        indexArn = arn,
        creationTime = Instant.now(),
        dataType = dataType,
        dimension = dimension,
        distanceMetric = distanceMetric,
        metadataConfiguration = metadataConfiguration,
        encryptionConfiguration = encryptionConfiguration,
        tags = tags.toMutableMap(),
      )
    bucket.indexes[indexName] = created
    indexesByArn[arn] = created
    return created
  }

  fun listIndexes(
    bucket: VectorBucketRecord?,
    prefix: String?,
  ): List<VectorIndexRecord> {
    val indexes =
      if (bucket != null) {
        bucket.indexes.values
      } else {
        indexesByArn.values
      }
    return indexes
      .asSequence()
      .filter { prefix.isNullOrBlank() || it.indexName.startsWith(prefix) }
      .sortedBy { it.indexName }
      .toList()
  }

  fun getIndex(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ): VectorIndexRecord? {
    indexArn?.let { return indexesByArn[it] }
    if (vectorBucketName == null || indexName == null) return null
    return bucketsByName[vectorBucketName]?.indexes?.get(indexName)
  }

  @Synchronized
  fun deleteIndex(
    vectorBucketName: String?,
    indexName: String?,
    indexArn: String?,
  ): Boolean {
    val index = getIndex(vectorBucketName, indexName, indexArn) ?: return false
    bucketsByName[index.vectorBucketName]?.indexes?.remove(index.indexName)
    indexesByArn.remove(index.indexArn)
    return true
  }

  fun putVectors(
    index: VectorIndexRecord,
    vectors: List<VectorRecord>,
  ) {
    vectors.forEach { index.vectors[it.key] = it }
  }

  fun getVectors(
    index: VectorIndexRecord,
    keys: List<String>,
  ): List<VectorRecord> = keys.mapNotNull { index.vectors[it] }

  fun listVectors(index: VectorIndexRecord): List<VectorRecord> = index.vectors.values.sortedBy { it.key }

  fun deleteVectors(
    index: VectorIndexRecord,
    keys: List<String>,
  ) {
    keys.forEach { index.vectors.remove(it) }
  }

  fun getVectorBucketPolicy(bucket: VectorBucketRecord): String? = bucket.policy

  fun putVectorBucketPolicy(
    bucket: VectorBucketRecord,
    policy: String,
  ) {
    bucket.policy = policy
  }

  fun deleteVectorBucketPolicy(bucket: VectorBucketRecord) {
    bucket.policy = null
  }

  fun listTagsForResource(resourceArn: String): Map<String, String> = resolveResourceTags(resourceArn).toMap()

  fun tagResource(
    resourceArn: String,
    tags: Map<String, String>,
  ) {
    resolveResourceTags(resourceArn).putAll(tags)
  }

  fun untagResource(
    resourceArn: String,
    tagKeys: List<String>,
  ) {
    val tags = resolveResourceTags(resourceArn)
    tagKeys.forEach(tags::remove)
  }

  private fun resolveResourceTags(resourceArn: String): MutableMap<String, String> {
    bucketsByArn[resourceArn]?.let { return it.tags }
    indexesByArn[resourceArn]?.let { return it.tags }
    throw NoSuchElementException(resourceArn)
  }

  private fun vectorBucketArn(vectorBucketName: String): String = "arn:aws:s3vectors:$region:000000000000:vector-bucket/$vectorBucketName"

  private fun indexArn(
    vectorBucketName: String,
    indexName: String,
  ): String = "arn:aws:s3vectors:$region:000000000000:index/$vectorBucketName/$indexName"
}

data class VectorBucketRecord(
  val vectorBucketName: String,
  val vectorBucketArn: String,
  val creationTime: Instant,
  val encryptionConfiguration: Map<String, Any?>?,
  val tags: MutableMap<String, String>,
  var policy: String? = null,
  val indexes: MutableMap<String, VectorIndexRecord> = ConcurrentHashMap(),
)

data class VectorIndexRecord(
  val vectorBucketName: String,
  val indexName: String,
  val indexArn: String,
  val creationTime: Instant,
  val dataType: String,
  val dimension: Int,
  val distanceMetric: String,
  val metadataConfiguration: Map<String, Any?>?,
  val encryptionConfiguration: Map<String, Any?>?,
  val tags: MutableMap<String, String>,
  val vectors: MutableMap<String, VectorRecord> = ConcurrentHashMap(),
)

data class VectorRecord(
  val key: String,
  val data: List<Double>,
  val metadata: Map<String, String>?,
)
