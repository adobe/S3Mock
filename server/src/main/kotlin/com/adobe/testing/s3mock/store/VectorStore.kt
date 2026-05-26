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

import com.adobe.testing.s3mock.dto.EncryptionConfiguration
import com.adobe.testing.s3mock.dto.MetadataConfiguration
import com.adobe.testing.s3mock.dto.PutInputVector
import com.adobe.testing.s3mock.dto.VectorData
import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

open class VectorStore(
  rootFolder: File,
  private val region: String,
  private val objectMapper: ObjectMapper,
) {
  private val rootPath: Path = rootFolder.toPath().resolve(VECTOR_ROOT).toAbsolutePath().normalize()
  private val bucketLockStore: MutableMap<String, Any> = ConcurrentHashMap<String, Any>()
  private val indexLockStore: MutableMap<UUID, Any> = ConcurrentHashMap<UUID, Any>()

  init {
    rootPath.createDirectories()
    loadExisting()
  }

  fun createVectorBucket(
    vectorBucketName: String,
    encryptionConfiguration: EncryptionConfiguration?,
    tags: Map<String, String>,
  ): VectorBucketMetadata {
    check(!doesVectorBucketExist(vectorBucketName))
    val bucketPath = bucketPath(vectorBucketName)
    bucketLockStore.putIfAbsent(vectorBucketName, Any())
    synchronized(bucketLockStore[vectorBucketName]!!) {
      bucketPath.createDirectories()
      val metadata =
        VectorBucketMetadata(
          vectorBucketName = vectorBucketName,
          vectorBucketArn = vectorBucketArn(region, vectorBucketName),
          creationTime = System.currentTimeMillis() / 1000,
          encryptionConfiguration = encryptionConfiguration,
          path = bucketPath,
          tags = tags,
        )
      writeBucketMetadata(metadata)
      return metadata
    }
  }

  fun doesVectorBucketExist(vectorBucketName: String): Boolean = bucketMetadataPath(vectorBucketName).exists()

  fun getVectorBucket(vectorBucketName: String): VectorBucketMetadata {
    check(doesVectorBucketExist(vectorBucketName))
    synchronized(bucketLockStore[vectorBucketName]!!) {
      return readBucketMetadata(vectorBucketName)
    }
  }

  fun getVectorBucketByArn(vectorBucketArn: String): VectorBucketMetadata = getVectorBucket(parseVectorBucketArn(vectorBucketArn))

  fun listVectorBuckets(
    prefix: String?,
    maxResults: Int,
    nextToken: String?,
  ): Pair<List<VectorBucketMetadata>, String?> {
    val startIndex = parseToken(nextToken)
    val buckets =
      rootPath
        .listDirectoryEntries()
        .filter { it.isDirectory() }
        .map { it.fileName.toString() }
        .sorted()
        .filter { prefix == null || it.startsWith(prefix) }
        .map { getVectorBucket(it) }

    val page = buckets.drop(startIndex).take(maxResults)
    val newToken = if (startIndex + page.size < buckets.size) encodeToken(startIndex + page.size) else null
    return page to newToken
  }

  fun deleteVectorBucket(vectorBucketName: String): Boolean {
    check(doesVectorBucketExist(vectorBucketName))
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val metadata = readBucketMetadata(vectorBucketName)
      if (metadata.indexes.isNotEmpty()) {
        return false
      }
      metadata.path.toFile().deleteRecursively()
      bucketLockStore.remove(vectorBucketName)
      return true
    }
  }

  fun putVectorBucketPolicy(
    vectorBucketName: String,
    policy: String,
  ) {
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val metadata = readBucketMetadata(vectorBucketName)
      writeBucketMetadata(metadata.copy(policy = policy))
    }
  }

  fun getVectorBucketPolicy(vectorBucketName: String): String? = getVectorBucket(vectorBucketName).policy

  fun deleteVectorBucketPolicy(vectorBucketName: String) {
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val metadata = readBucketMetadata(vectorBucketName)
      writeBucketMetadata(metadata.copy(policy = null))
    }
  }

  fun putVectorBucketTags(
    vectorBucketName: String,
    tags: Map<String, String>,
  ) {
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val metadata = readBucketMetadata(vectorBucketName)
      writeBucketMetadata(metadata.copy(tags = metadata.tags + tags))
    }
  }

  fun createIndex(
    vectorBucketName: String,
    indexName: String,
    dataType: String,
    dimension: Int,
    distanceMetric: String,
    encryptionConfiguration: EncryptionConfiguration?,
    metadataConfiguration: MetadataConfiguration?,
    tags: Map<String, String>,
  ): VectorIndexMetadata {
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val bucketMetadata = readBucketMetadata(vectorBucketName)
      check(bucketMetadata.findIndexId(indexName) == null)
      val indexId = bucketMetadata.addIndex(indexName)
      val indexPath = indexPath(bucketMetadata, indexId)
      indexPath.createDirectories()
      indexPath.resolve(VECTORS_FOLDER).createDirectories()
      val indexMetadata =
        VectorIndexMetadata(
          id = indexId,
          indexName = indexName,
          indexArn = vectorIndexArn(region, vectorBucketName, indexName),
          vectorBucketName = vectorBucketName,
          creationTime = System.currentTimeMillis() / 1000,
          dataType = dataType,
          dimension = dimension,
          distanceMetric = distanceMetric,
          encryptionConfiguration = encryptionConfiguration,
          metadataConfiguration = metadataConfiguration,
          tags = tags,
          path = indexPath,
        )
      writeBucketMetadata(bucketMetadata)
      writeIndexMetadata(indexMetadata)
      indexLockStore.putIfAbsent(indexId, Any())
      return indexMetadata
    }
  }

  fun getIndex(
    vectorBucketName: String,
    indexName: String,
  ): VectorIndexMetadata {
    val bucketMetadata = getVectorBucket(vectorBucketName)
    val indexId = bucketMetadata.findIndexId(indexName) ?: error("Index does not exist")
    indexLockStore.putIfAbsent(indexId, Any())
    synchronized(indexLockStore[indexId]!!) {
      return readIndexMetadata(bucketMetadata, indexId)
    }
  }

  fun getIndexByArn(indexArn: String): VectorIndexMetadata {
    val parsed = parseVectorIndexArn(indexArn)
    return getIndex(parsed.first, parsed.second)
  }

  fun deleteIndex(
    vectorBucketName: String,
    indexName: String,
  ): Boolean {
    synchronized(bucketLockStore[vectorBucketName]!!) {
      val bucketMetadata = readBucketMetadata(vectorBucketName)
      val indexId = bucketMetadata.findIndexId(indexName) ?: return false
      val indexMetadata = readIndexMetadata(bucketMetadata, indexId)
      if (indexMetadata.vectors.isNotEmpty()) {
        return false
      }
      indexMetadata.path.toFile().deleteRecursively()
      bucketMetadata.removeIndex(indexName)
      writeBucketMetadata(bucketMetadata)
      indexLockStore.remove(indexId)
      return true
    }
  }

  fun listIndexes(
    vectorBucketName: String,
    prefix: String?,
    maxResults: Int,
    nextToken: String?,
  ): Pair<List<VectorIndexMetadata>, String?> {
    val startIndex = parseToken(nextToken)
    val allIndexes =
      getVectorBucket(vectorBucketName)
        .indexes
        .keys
        .sorted()
        .filter { prefix == null || it.startsWith(prefix) }
        .map { getIndex(vectorBucketName, it) }

    val page = allIndexes.drop(startIndex).take(maxResults)
    val newToken = if (startIndex + page.size < allIndexes.size) encodeToken(startIndex + page.size) else null
    return page to newToken
  }

  fun putIndexTags(
    vectorBucketName: String,
    indexName: String,
    tags: Map<String, String>,
  ) {
    val index = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[index.id]!!) {
      writeIndexMetadata(index.copy(tags = index.tags + tags))
    }
  }

  fun deleteTags(
    resourceArn: String,
    tagKeys: List<String>,
  ) {
    if (resourceArn.contains("/index/")) {
      val parsed = parseVectorIndexArn(resourceArn)
      val index = getIndex(parsed.first, parsed.second)
      synchronized(indexLockStore[index.id]!!) {
        writeIndexMetadata(index.copy(tags = index.tags - tagKeys.toSet()))
      }
      return
    }

    val bucketName = parseVectorBucketArn(resourceArn)
    synchronized(bucketLockStore[bucketName]!!) {
      val bucketMetadata = readBucketMetadata(bucketName)
      writeBucketMetadata(bucketMetadata.copy(tags = bucketMetadata.tags - tagKeys.toSet()))
    }
  }

  fun listTags(resourceArn: String): Map<String, String> {
    if (resourceArn.contains("/index/")) {
      val parsed = parseVectorIndexArn(resourceArn)
      return getIndex(parsed.first, parsed.second).tags
    }
    return getVectorBucketByArn(resourceArn).tags
  }

  fun putVectors(
    vectorBucketName: String,
    indexName: String,
    vectors: List<PutInputVector>,
  ) {
    val indexMetadata = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[indexMetadata.id]!!) {
      var mutableIndex = indexMetadata
      vectors.forEach {
        val vectorId = mutableIndex.addVector(it.key)
        val storedVector = StoredVector(it.key, it.data, it.metadata)
        objectMapper.writeValue(vectorPath(mutableIndex, vectorId).toFile(), storedVector)
      }
      writeIndexMetadata(mutableIndex)
    }
  }

  fun getVectors(
    vectorBucketName: String,
    indexName: String,
    keys: List<String>,
  ): List<StoredVector> {
    val indexMetadata = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[indexMetadata.id]!!) {
      return keys.mapNotNull { key ->
        val vectorId = indexMetadata.findVectorId(key) ?: return@mapNotNull null
        readVector(indexMetadata, vectorId)
      }
    }
  }

  fun listVectors(
    vectorBucketName: String,
    indexName: String,
    maxResults: Int,
    nextToken: String?,
    segmentCount: Int?,
    segmentIndex: Int?,
  ): Pair<List<StoredVector>, String?> {
    val indexMetadata = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[indexMetadata.id]!!) {
      val startIndex = parseToken(nextToken)
      val filteredKeys =
        indexMetadata.vectors.keys
          .sorted()
          .filter { key ->
            if (segmentCount == null || segmentIndex == null) {
              true
            } else {
              Math.floorMod(key.hashCode(), segmentCount) == segmentIndex
            }
          }

      val pageKeys = filteredKeys.drop(startIndex).take(maxResults)
      val vectors =
        pageKeys.mapNotNull { key ->
          val vectorId = indexMetadata.findVectorId(key) ?: return@mapNotNull null
          readVector(indexMetadata, vectorId)
        }
      val token = if (startIndex + vectors.size < filteredKeys.size) encodeToken(startIndex + vectors.size) else null
      return vectors to token
    }
  }

  fun deleteVectors(
    vectorBucketName: String,
    indexName: String,
    keys: List<String>,
  ) {
    val indexMetadata = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[indexMetadata.id]!!) {
      keys.forEach { key ->
        val vectorId = indexMetadata.removeVector(key) ?: return@forEach
        vectorPath(indexMetadata, vectorId).toFile().delete()
      }
      writeIndexMetadata(indexMetadata)
    }
  }

  fun queryVectors(
    vectorBucketName: String,
    indexName: String,
    queryVector: VectorData,
    topK: Int,
  ): List<Pair<StoredVector, Double>> {
    val queryData = queryVector.float32 ?: emptyList()
    val indexMetadata = getIndex(vectorBucketName, indexName)
    synchronized(indexLockStore[indexMetadata.id]!!) {
      return indexMetadata.vectors.values
        .mapNotNull { id -> readVector(indexMetadata, id) }
        .map { it to distance(indexMetadata.distanceMetric, queryData, it.data.float32 ?: emptyList()) }
        .sortedBy { it.second }
        .take(topK)
    }
  }

  private fun distance(
    metric: String,
    queryVector: List<Float>,
    candidateVector: List<Float>,
  ): Double {
    return if (metric == DISTANCE_COSINE) {
      cosineDistance(queryVector, candidateVector)
    } else {
      euclideanDistance(queryVector, candidateVector)
    }
  }

  private fun euclideanDistance(
    queryVector: List<Float>,
    candidateVector: List<Float>,
  ): Double =
    queryVector
      .zip(candidateVector)
      .sumOf { (q, c) -> (q - c).toDouble() * (q - c).toDouble() }

  private fun cosineDistance(
    queryVector: List<Float>,
    candidateVector: List<Float>,
  ): Double {
    val dotProduct = queryVector.zip(candidateVector).sumOf { (q, c) -> (q * c).toDouble() }
    val queryMagnitude = kotlin.math.sqrt(queryVector.sumOf { (it * it).toDouble() })
    val candidateMagnitude = kotlin.math.sqrt(candidateVector.sumOf { (it * it).toDouble() })
    if (queryMagnitude == 0.0 || candidateMagnitude == 0.0) {
      return 1.0
    }
    return 1.0 - (dotProduct / (queryMagnitude * candidateMagnitude))
  }

  private fun loadExisting() {
    if (!rootPath.exists()) {
      return
    }

    rootPath
      .listDirectoryEntries()
      .filter { it.isDirectory() }
      .forEach { bucketDir ->
        val bucketName = bucketDir.fileName.toString()
        if (!bucketMetadataPath(bucketName).exists()) {
          return@forEach
        }
        bucketLockStore.putIfAbsent(bucketName, Any())
        val bucketMetadata = readBucketMetadata(bucketName)
        bucketMetadata.indexes.values.forEach { indexId ->
          indexLockStore.putIfAbsent(indexId, Any())
        }
      }
  }

  private fun bucketMetadataPath(vectorBucketName: String): Path = bucketPath(vectorBucketName).resolve(VECTOR_BUCKET_META_FILE)

  private fun readBucketMetadata(vectorBucketName: String): VectorBucketMetadata =
    try {
      objectMapper.readValue(bucketMetadataPath(vectorBucketName).toFile(), VectorBucketMetadata::class.java)
    } catch (e: IOException) {
      throw IllegalStateException("Could not read vector bucket metadata-file $vectorBucketName", e)
    }

  private fun writeBucketMetadata(bucketMetadata: VectorBucketMetadata) {
    try {
      objectMapper.writeValue(bucketMetadataPath(bucketMetadata.vectorBucketName).toFile(), bucketMetadata)
    } catch (e: IOException) {
      throw IllegalStateException("Could not write vector bucket metadata-file ${bucketMetadata.vectorBucketName}", e)
    }
  }

  private fun indexPath(
    bucketMetadata: VectorBucketMetadata,
    indexId: UUID,
  ): Path = bucketMetadata.path.resolve(INDEX_FOLDER).resolve(indexId.toString())

  private fun indexMetadataPath(
    bucketMetadata: VectorBucketMetadata,
    indexId: UUID,
  ): Path = indexPath(bucketMetadata, indexId).resolve(VECTOR_INDEX_META_FILE)

  private fun readIndexMetadata(
    bucketMetadata: VectorBucketMetadata,
    indexId: UUID,
  ): VectorIndexMetadata =
    try {
      objectMapper.readValue(indexMetadataPath(bucketMetadata, indexId).toFile(), VectorIndexMetadata::class.java)
    } catch (e: IOException) {
      throw IllegalStateException("Could not read vector index metadata-file $indexId", e)
    }

  private fun writeIndexMetadata(indexMetadata: VectorIndexMetadata) {
    try {
      objectMapper.writeValue(indexMetadataPath(getVectorBucket(indexMetadata.vectorBucketName), indexMetadata.id).toFile(), indexMetadata)
    } catch (e: IOException) {
      throw IllegalStateException("Could not write vector index metadata-file ${indexMetadata.id}", e)
    }
  }

  private fun vectorPath(
    indexMetadata: VectorIndexMetadata,
    vectorId: UUID,
  ): Path = indexMetadata.path.resolve(VECTORS_FOLDER).resolve("$vectorId.json")

  private fun readVector(
    indexMetadata: VectorIndexMetadata,
    vectorId: UUID,
  ): StoredVector? {
    val path = vectorPath(indexMetadata, vectorId)
    if (!path.exists()) {
      return null
    }

    return try {
      objectMapper.readValue(path.toFile(), StoredVector::class.java)
    } catch (e: IOException) {
      throw IllegalStateException("Could not read vector data-file $vectorId", e)
    }
  }

  private fun bucketPath(vectorBucketName: String): Path {
    val candidate = rootPath.resolve(vectorBucketName).normalize()
    require(candidate.startsWith(rootPath)) { "Invalid vector bucket name (path traversal detected)." }
    return candidate
  }

  private fun parseToken(nextToken: String?): Int {
    if (nextToken.isNullOrBlank()) {
      return 0
    }
    val decoded = String(Base64.getDecoder().decode(nextToken), StandardCharsets.UTF_8)
    return decoded.toIntOrNull() ?: 0
  }

  private fun encodeToken(index: Int): String = Base64.getEncoder().encodeToString(index.toString().toByteArray(StandardCharsets.UTF_8))

  private fun parseVectorBucketArn(vectorBucketArn: String): String {
    val marker = ":bucket/"
    val markerIndex = vectorBucketArn.indexOf(marker)
    require(markerIndex > -1) { "Invalid vector bucket ARN" }
    return vectorBucketArn.substring(markerIndex + marker.length)
  }

  private fun parseVectorIndexArn(indexArn: String): Pair<String, String> {
    val bucketMarker = ":bucket/"
    val indexMarker = "/index/"
    val bucketStart = indexArn.indexOf(bucketMarker)
    val indexStart = indexArn.indexOf(indexMarker)
    require(bucketStart > -1 && indexStart > -1) { "Invalid vector index ARN" }
    val bucketName = indexArn.substring(bucketStart + bucketMarker.length, indexStart)
    val indexName = indexArn.substring(indexStart + indexMarker.length)
    return bucketName to indexName
  }

  companion object {
    private const val VECTOR_ROOT = "s3vectors"
    private const val VECTOR_BUCKET_META_FILE = "vectorBucketMetadata.json"
    private const val VECTOR_INDEX_META_FILE = "indexMetadata.json"
    private const val INDEX_FOLDER = "indexes"
    private const val VECTORS_FOLDER = "vectors"
    private const val DISTANCE_COSINE = "cosine"
  }
}
