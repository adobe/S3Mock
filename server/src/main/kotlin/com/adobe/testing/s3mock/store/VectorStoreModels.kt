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
import com.adobe.testing.s3mock.dto.VectorData
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.JsonNode
import java.nio.file.Path
import java.util.UUID

private const val DEFAULT_ACCOUNT_ID = "123456789012"

data class VectorBucketMetadata(
  val vectorBucketName: String,
  val vectorBucketArn: String,
  val creationTime: Long,
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val path: Path,
  val policy: String? = null,
  val tags: Map<String, String> = emptyMap(),
  @param:JsonProperty("indexes")
  private val _indexes: MutableMap<String, UUID> = mutableMapOf(),
) {
  val indexes: Map<String, UUID>
    get() = java.util.Collections.unmodifiableMap(_indexes)

  fun addIndex(indexName: String): UUID {
    val existing = _indexes[indexName]
    if (existing != null) {
      return existing
    }
    val id = UUID.randomUUID()
    _indexes[indexName] = id
    return id
  }

  fun removeIndex(indexName: String): UUID? = _indexes.remove(indexName)

  fun findIndexId(indexName: String): UUID? = _indexes[indexName]
}

data class VectorIndexMetadata(
  val id: UUID,
  val indexName: String,
  val indexArn: String,
  val vectorBucketName: String,
  val creationTime: Long,
  val dataType: String,
  val dimension: Int,
  val distanceMetric: String,
  val encryptionConfiguration: EncryptionConfiguration? = null,
  val metadataConfiguration: MetadataConfiguration? = null,
  val tags: Map<String, String> = emptyMap(),
  val path: Path,
  @param:JsonProperty("vectors")
  private val _vectors: MutableMap<String, UUID> = mutableMapOf(),
) {
  val vectors: Map<String, UUID>
    get() = java.util.Collections.unmodifiableMap(_vectors)

  fun addVector(key: String): UUID {
    val existing = _vectors[key]
    if (existing != null) {
      return existing
    }
    val id = UUID.randomUUID()
    _vectors[key] = id
    return id
  }

  fun removeVector(key: String): UUID? = _vectors.remove(key)

  fun findVectorId(key: String): UUID? = _vectors[key]
}

data class StoredVector(
  val key: String,
  val data: VectorData,
  val metadata: JsonNode? = null,
)

fun vectorBucketArn(
  region: String,
  bucketName: String,
): String = "arn:aws:s3vectors:$region:$DEFAULT_ACCOUNT_ID:bucket/$bucketName"

fun vectorIndexArn(
  region: String,
  bucketName: String,
  indexName: String,
): String = "arn:aws:s3vectors:$region:$DEFAULT_ACCOUNT_ID:bucket/$bucketName/index/$indexName"
