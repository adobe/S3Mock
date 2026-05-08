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

import com.adobe.testing.s3mock.store.VectorIndexRecord
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonSerialize
import java.time.Instant

/**
 * Summary of a vector index for list operations.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_IndexSummary_s3vectors.html)
 */
data class VectorIndexSummary(
  @param:JsonProperty("CreationTime")
  @param:JsonSerialize(using = InstantSerializer::class)
  val creationTime: Instant?,
  @param:JsonProperty("IndexArn")
  val indexArn: String?,
  @param:JsonProperty("IndexName")
  val indexName: String?,
  @param:JsonProperty("VectorBucketName")
  val vectorBucketName: String?,
) {
  companion object {
    fun from(index: VectorIndexRecord): VectorIndexSummary =
      VectorIndexSummary(
        creationTime = index.creationTime,
        indexArn = index.indexArn,
        indexName = index.indexName,
        vectorBucketName = index.vectorBucketName,
      )
  }
}
