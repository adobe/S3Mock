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
 * Stores vector buckets and their metadata on the filesystem.
 * Layout: `<rootFolder>/vectors/<bucketName>/bucketMetadata.json`
 */
class VectorBucketStore(
  private val vectorsRoot: File,
  private val objectMapper: ObjectMapper,
) {
  private val lockStore: MutableMap<String, Any> = ConcurrentHashMap()

  fun createVectorBucket(
    name: String,
    sseType: String?,
    kmsKeyArn: String?,
    tags: Map<String, String>,
  ): VectorBucketMetadata {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      if (doesBucketExist(name)) throw S3VectorsException.VECTOR_BUCKET_ALREADY_EXISTS
      val bucketDir = getBucketDir(name)
      bucketDir.mkdirs()
      val metadata =
        VectorBucketMetadata(
          name = name,
          creationTime = System.currentTimeMillis(),
          sseType = sseType,
          kmsKeyArn = kmsKeyArn,
          tags = tags,
          path = bucketDir.toPath(),
        )
      writeBucketMetadata(metadata)
      return metadata
    }
  }

  fun getBucketMetadata(name: String): VectorBucketMetadata {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      val metaFile = getBucketDir(name).resolve(BUCKET_META_FILE)
      if (!metaFile.exists()) throw S3VectorsException.VECTOR_BUCKET_NOT_FOUND
      return objectMapper.readValue(metaFile, VectorBucketMetadata::class.java).copy(path = metaFile.parentFile.toPath())
    }
  }

  fun doesBucketExist(name: String): Boolean = getBucketDir(name).resolve(BUCKET_META_FILE).exists()

  fun listBuckets(): List<VectorBucketMetadata> {
    try {
      val root = vectorsRoot.toPath()
      if (!root.toFile().exists()) return emptyList()
      return root
        .listDirectoryEntries()
        .filter { it.isDirectory() }
        .filter { it.resolve(BUCKET_META_FILE).toFile().exists() }
        .map { bucketDir ->
          objectMapper
            .readValue(bucketDir.resolve(BUCKET_META_FILE).toFile(), VectorBucketMetadata::class.java)
            .copy(path = bucketDir)
        }.sortedBy { it.name }
    } catch (e: IOException) {
      throw IllegalStateException("Could not list vector buckets", e)
    }
  }

  fun updateTags(
    name: String,
    tags: Map<String, String>,
  ) {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      val meta = getBucketMetadata(name)
      writeBucketMetadata(meta.copy(tags = meta.tags + tags))
    }
  }

  fun removeTags(
    name: String,
    tagKeys: List<String>,
  ) {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      val meta = getBucketMetadata(name)
      writeBucketMetadata(meta.copy(tags = meta.tags.filterKeys { it !in tagKeys }))
    }
  }

  fun storePolicy(
    name: String,
    policy: String,
  ) {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      val policyFile = getBucketDir(name).resolve(BUCKET_POLICY_FILE)
      policyFile.writeText(policy)
    }
  }

  fun getPolicy(name: String): String? {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      val policyFile = getBucketDir(name).resolve(BUCKET_POLICY_FILE)
      return if (policyFile.exists()) policyFile.readText() else null
    }
  }

  fun deletePolicy(name: String) {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      getBucketDir(name).resolve(BUCKET_POLICY_FILE).delete()
    }
  }

  fun deleteBucket(name: String) {
    lockStore.putIfAbsent(name, Any())
    synchronized(lockStore[name]!!) {
      getBucketDir(name).deleteRecursively()
      lockStore.remove(name)
    }
  }

  fun hasIndexes(name: String): Boolean {
    val indexesDir = getBucketDir(name).resolve(INDEXES_DIR)
    return indexesDir.exists() &&
      indexesDir.listFiles()?.any { it.isDirectory && it.resolve("indexMetadata.json").exists() } == true
  }

  fun getBucketDir(name: String): File = vectorsRoot.resolve(name)

  private fun writeBucketMetadata(metadata: VectorBucketMetadata) {
    val metaFile = getBucketDir(metadata.name).resolve(BUCKET_META_FILE)
    objectMapper.writeValue(metaFile, metadata)
  }

  companion object {
    const val BUCKET_META_FILE = "bucketMetadata.json"
    const val BUCKET_POLICY_FILE = "bucketPolicy.json"
    const val INDEXES_DIR = "indexes"
  }
}
