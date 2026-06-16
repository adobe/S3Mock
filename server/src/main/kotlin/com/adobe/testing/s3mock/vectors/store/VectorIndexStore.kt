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
package com.adobe.testing.s3mock.vectors.store

import com.adobe.testing.s3mock.vectors.S3VectorsException
import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

/**
 * Stores vector indexes and their metadata on the filesystem.
 * Layout: `<vectorsRoot>/<bucket>/indexes/<indexName>/indexMetadata.json`
 */
class VectorIndexStore(
  private val vectorBucketStore: VectorBucketStore,
  private val objectMapper: ObjectMapper,
) {
  private val lockStore: MutableMap<String, Any> = ConcurrentHashMap()

  private fun lockKey(
    bucketName: String,
    indexName: String,
  ) = "$bucketName/$indexName"

  fun createIndex(
    bucketName: String,
    indexName: String,
    dataType: String,
    dimension: Int,
    distanceMetric: String,
    sseType: String?,
    kmsKeyArn: String?,
    nonFilterableMetadataKeys: List<String>,
    tags: Map<String, String>,
  ): VectorIndexMetadata {
    val key = lockKey(bucketName, indexName)
    lockStore.putIfAbsent(key, Any())
    synchronized(lockStore[key]!!) {
      if (doesIndexExist(bucketName, indexName)) throw S3VectorsException.INDEX_ALREADY_EXISTS
      val indexDir = getIndexDir(bucketName, indexName)
      indexDir.mkdirs()
      val metadata =
        VectorIndexMetadata(
          name = indexName,
          vectorBucketName = bucketName,
          dataType = dataType,
          dimension = dimension,
          distanceMetric = distanceMetric,
          creationTime = System.currentTimeMillis(),
          sseType = sseType,
          kmsKeyArn = kmsKeyArn,
          nonFilterableMetadataKeys = nonFilterableMetadataKeys,
          tags = tags,
          path = indexDir.toPath(),
        )
      writeIndexMetadata(metadata)
      return metadata
    }
  }

  fun getIndexMetadata(
    bucketName: String,
    indexName: String,
  ): VectorIndexMetadata {
    val key = lockKey(bucketName, indexName)
    lockStore.putIfAbsent(key, Any())
    synchronized(lockStore[key]!!) {
      val metaFile = getIndexDir(bucketName, indexName).resolve(INDEX_META_FILE)
      if (!metaFile.exists()) throw S3VectorsException.INDEX_NOT_FOUND
      return objectMapper
        .readValue(metaFile, VectorIndexMetadata::class.java)
        .copy(path = metaFile.parentFile.toPath())
    }
  }

  fun doesIndexExist(
    bucketName: String,
    indexName: String,
  ): Boolean = getIndexDir(bucketName, indexName).resolve(INDEX_META_FILE).exists()

  fun listIndexes(bucketName: String): List<VectorIndexMetadata> {
    try {
      val indexesDir = vectorBucketStore.getBucketDir(bucketName).resolve(VectorBucketStore.INDEXES_DIR).toPath()
      if (!indexesDir.toFile().exists()) return emptyList()
      return indexesDir
        .listDirectoryEntries()
        .filter { it.isDirectory() }
        .filter { it.resolve(INDEX_META_FILE).toFile().exists() }
        .map { indexDir ->
          objectMapper
            .readValue(indexDir.resolve(INDEX_META_FILE).toFile(), VectorIndexMetadata::class.java)
            .copy(path = indexDir)
        }.sortedBy { it.name }
    } catch (e: IOException) {
      throw IllegalStateException("Could not list vector indexes for bucket $bucketName", e)
    }
  }

  fun updateTags(
    bucketName: String,
    indexName: String,
    tags: Map<String, String>,
  ) {
    val key = lockKey(bucketName, indexName)
    lockStore.putIfAbsent(key, Any())
    synchronized(lockStore[key]!!) {
      val meta = getIndexMetadata(bucketName, indexName)
      writeIndexMetadata(meta.copy(tags = meta.tags + tags))
    }
  }

  fun removeTags(
    bucketName: String,
    indexName: String,
    tagKeys: List<String>,
  ) {
    val key = lockKey(bucketName, indexName)
    lockStore.putIfAbsent(key, Any())
    synchronized(lockStore[key]!!) {
      val meta = getIndexMetadata(bucketName, indexName)
      writeIndexMetadata(meta.copy(tags = meta.tags.filterKeys { it !in tagKeys }))
    }
  }

  fun deleteIndex(
    bucketName: String,
    indexName: String,
  ) {
    val key = lockKey(bucketName, indexName)
    lockStore.putIfAbsent(key, Any())
    synchronized(lockStore[key]!!) {
      getIndexDir(bucketName, indexName).deleteRecursively()
      lockStore.remove(key)
    }
  }

  fun getIndexDir(
    bucketName: String,
    indexName: String,
  ): File = vectorBucketStore.getBucketDir(bucketName).resolve(VectorBucketStore.INDEXES_DIR).resolve(indexName)

  private fun writeIndexMetadata(metadata: VectorIndexMetadata) {
    val metaFile = getIndexDir(metadata.vectorBucketName, metadata.name).resolve(INDEX_META_FILE)
    objectMapper.writeValue(metaFile, metadata)
  }

  companion object {
    const val INDEX_META_FILE = "indexMetadata.json"
    const val VECTORS_DIR = "vectors"
  }
}
