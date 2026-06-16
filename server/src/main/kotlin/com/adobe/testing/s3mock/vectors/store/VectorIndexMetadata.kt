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

import com.fasterxml.jackson.annotation.JsonIgnore
import java.nio.file.Path

/**
 * Persistent metadata for a vector index, stored as JSON on disk.
 * `path` is transient — it is set by the store after reading from disk, never written to JSON.
 */
data class VectorIndexMetadata(
  val name: String,
  val vectorBucketName: String,
  val dataType: String,
  val dimension: Int,
  val distanceMetric: String,
  val creationTime: Long,
  val sseType: String?,
  val kmsKeyArn: String?,
  val nonFilterableMetadataKeys: List<String>,
  val tags: Map<String, String>,
  @JsonIgnore val path: Path? = null,
)
