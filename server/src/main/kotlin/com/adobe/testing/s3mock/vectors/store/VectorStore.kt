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

import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

/**
 * Stores individual vectors on the filesystem.
 * Layout: `<indexDir>/vectors/<sha256(key)>/{key.txt, data.f32, metadata.json}`
 *
 * The vector key is hashed (SHA-256) to avoid filesystem issues with arbitrary Unicode keys.
 * The original key is preserved in `key.txt`.
 */
class VectorStore(
  private val vectorIndexStore: VectorIndexStore,
  private val objectMapper: ObjectMapper,
) {
  private val lockStore: MutableMap<String, Any> = ConcurrentHashMap()

  data class StoredVector(
    val key: String,
    val floats: FloatArray?,
    val metadata: JsonNode?,
  )

  fun putVector(
    bucketName: String,
    indexName: String,
    key: String,
    floats: FloatArray,
    metadata: Map<String, Any?>?,
  ) {
    val lockKey = lockKey(bucketName, indexName, key)
    lockStore.putIfAbsent(lockKey, Any())
    synchronized(lockStore[lockKey]!!) {
      val dir = getVectorDir(bucketName, indexName, key)
      dir.mkdirs()
      dir.resolve(KEY_FILE).writeText(key, Charsets.UTF_8)
      dir.resolve(DATA_FILE).writeBytes(floatsToBytes(floats))
      if (metadata != null) {
        objectMapper.writeValue(dir.resolve(METADATA_FILE), metadata)
      } else {
        dir.resolve(METADATA_FILE).delete()
      }
    }
  }

  fun getVector(
    bucketName: String,
    indexName: String,
    key: String,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): StoredVector? {
    val dir = getVectorDir(bucketName, indexName, key)
    if (!dir.exists()) return null
    return readVectorDir(dir, returnData, returnMetadata)
  }

  fun vectorExists(
    bucketName: String,
    indexName: String,
    key: String,
  ): Boolean = getVectorDir(bucketName, indexName, key).resolve(KEY_FILE).exists()

  fun deleteVector(
    bucketName: String,
    indexName: String,
    key: String,
  ) {
    val lockKey = lockKey(bucketName, indexName, key)
    synchronized(lockStore[lockKey]!!) {
      getVectorDir(bucketName, indexName, key).deleteRecursively()
      lockStore.remove(lockKey)
    }
  }

  fun listVectors(
    bucketName: String,
    indexName: String,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): List<StoredVector> {
    try {
      val vectorsDir =
        vectorIndexStore
          .getIndexDir(bucketName, indexName)
          .resolve(VectorIndexStore.VECTORS_DIR)
          .toPath()
      if (!vectorsDir.toFile().exists()) return emptyList()
      return vectorsDir
        .listDirectoryEntries()
        .filter { it.isDirectory() }
        .filter { it.resolve(KEY_FILE).toFile().exists() }
        .map { readVectorDir(it.toFile(), returnData, returnMetadata) }
        .sortedBy { it.key }
    } catch (e: IOException) {
      throw IllegalStateException("Could not list vectors for index $indexName", e)
    }
  }

  fun countVectors(
    bucketName: String,
    indexName: String,
  ): Int = listVectors(bucketName, indexName, returnData = false, returnMetadata = false).size

  private fun readVectorDir(
    dir: File,
    returnData: Boolean,
    returnMetadata: Boolean,
  ): StoredVector {
    val key = dir.resolve(KEY_FILE).readText(Charsets.UTF_8)
    val floats = if (returnData && dir.resolve(DATA_FILE).exists()) bytesToFloats(dir.resolve(DATA_FILE).readBytes()) else null
    val metadataNode =
      if (returnMetadata && dir.resolve(METADATA_FILE).exists()) {
        objectMapper.readTree(dir.resolve(METADATA_FILE))
      } else {
        null
      }
    return StoredVector(key, floats, metadataNode)
  }

  fun readAllForQuery(
    bucketName: String,
    indexName: String,
  ): List<StoredVector> {
    try {
      val vectorsDir =
        vectorIndexStore
          .getIndexDir(bucketName, indexName)
          .resolve(VectorIndexStore.VECTORS_DIR)
          .toPath()
      if (!vectorsDir.toFile().exists()) return emptyList()
      return vectorsDir
        .listDirectoryEntries()
        .filter { it.isDirectory() }
        .filter { it.resolve(KEY_FILE).toFile().exists() }
        .map { vectorDir ->
          val key = vectorDir.resolve(KEY_FILE).toFile().readText(Charsets.UTF_8)
          val floats =
            if (vectorDir.resolve(DATA_FILE).toFile().exists()) {
              bytesToFloats(vectorDir.resolve(DATA_FILE).toFile().readBytes())
            } else {
              null
            }
          val metadataNode =
            if (vectorDir.resolve(METADATA_FILE).toFile().exists()) {
              objectMapper.readTree(vectorDir.resolve(METADATA_FILE).toFile())
            } else {
              null
            }
          StoredVector(key, floats, metadataNode)
        }
    } catch (e: IOException) {
      throw IllegalStateException("Could not read vectors for query in index $indexName", e)
    }
  }

  private fun getVectorDir(
    bucketName: String,
    indexName: String,
    key: String,
  ): File {
    val hash = sha256Hex(key)
    return vectorIndexStore.getIndexDir(bucketName, indexName).resolve(VectorIndexStore.VECTORS_DIR).resolve(hash)
  }

  private fun lockKey(
    bucketName: String,
    indexName: String,
    key: String,
  ) = "$bucketName/$indexName/${sha256Hex(key)}"

  companion object {
    const val KEY_FILE = "key.txt"
    const val DATA_FILE = "data.f32"
    const val METADATA_FILE = "metadata.json"

    fun sha256Hex(input: String): String {
      val digest = MessageDigest.getInstance("SHA-256")
      val bytes = digest.digest(input.toByteArray(Charsets.UTF_8))
      return HexFormat.of().formatHex(bytes)
    }

    fun floatsToBytes(floats: FloatArray): ByteArray {
      val buf = ByteBuffer.allocate(floats.size * 4).order(ByteOrder.LITTLE_ENDIAN)
      floats.forEach { buf.putFloat(it) }
      return buf.array()
    }

    fun bytesToFloats(bytes: ByteArray): FloatArray {
      val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer()
      return FloatArray(buf.limit()) { buf.get() }
    }
  }
}
