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
import com.adobe.testing.s3mock.vectors.dto.QueryOutputVector
import com.adobe.testing.s3mock.vectors.dto.QueryVectorsResponse
import com.adobe.testing.s3mock.vectors.dto.VectorData
import com.adobe.testing.s3mock.vectors.store.VectorStore
import tools.jackson.databind.JsonNode
import kotlin.math.sqrt

open class VectorQueryService(
  private val vectorIndexService: VectorIndexService,
  private val vectorStore: VectorStore,
) {
  fun queryVectors(
    bucketNameOrArn: String?,
    indexNameOrArn: String,
    queryVector: VectorData,
    topK: Int,
    filter: JsonNode?,
    returnDistance: Boolean,
    returnMetadata: Boolean,
  ): QueryVectorsResponse {
    if (topK !in 1..100) throw S3VectorsException.INVALID_TOP_K
    val (bucketName, indexName) = vectorIndexService.resolveIndexId(bucketNameOrArn, indexNameOrArn)
    val indexMeta = vectorIndexService.requireIndex(bucketName, indexName)

    val query =
      queryVector.float32?.map { it.toFloat() }?.toFloatArray()
        ?: throw S3VectorsException.validation("queryVector.float32 is required.")
    if (query.size != indexMeta.dimension) throw S3VectorsException.dimensionMismatch(indexMeta.dimension, query.size)

    if (filter != null) {
      validateFilter(filter, indexMeta.nonFilterableMetadataKeys)
    }

    val allVectors = vectorStore.readAllForQuery(bucketName, indexName)

    data class Candidate(
      val key: String,
      val dist: Double,
      val metadata: JsonNode?,
    )

    val candidates = mutableListOf<Candidate>()
    for ((key, floats1, metadata) in allVectors) {
      val floats = floats1 ?: continue
      if (filter != null && !matchesFilter(metadata, filter)) continue
      val dist = computeDistance(query, floats, indexMeta.distanceMetric)
      candidates.add(Candidate(key, dist, metadata))
    }

    candidates.sortBy { it.dist }
    val top = candidates.take(topK)

    return QueryVectorsResponse(
      distanceMetric = indexMeta.distanceMetric,
      vectors =
        top.map { c ->
          QueryOutputVector(
            key = c.key,
            distance = if (returnDistance) c.dist else null,
            metadata = if (returnMetadata) c.metadata else null,
          )
        },
    )
  }

  private fun computeDistance(
    a: FloatArray,
    b: FloatArray,
    metric: String,
  ): Double =
    when (metric) {
      "euclidean" -> euclidean(a, b)
      "cosine" -> cosine(a, b)
      else -> euclidean(a, b)
    }

  private fun euclidean(
    a: FloatArray,
    b: FloatArray,
  ): Double {
    var sum = 0.0
    for (i in a.indices) {
      val diff = (a[i] - b[i]).toDouble()
      sum += diff * diff
    }
    return sqrt(sum)
  }

  private fun cosine(
    a: FloatArray,
    b: FloatArray,
  ): Double {
    var dot = 0.0
    var normA = 0.0
    var normB = 0.0
    for (i in a.indices) {
      dot += a[i].toDouble() * b[i].toDouble()
      normA += a[i].toDouble() * a[i].toDouble()
      normB += b[i].toDouble() * b[i].toDouble()
    }
    val denom = sqrt(normA) * sqrt(normB)
    return if (denom == 0.0) 1.0 else 1.0 - (dot / denom)
  }

  private fun validateFilter(
    filter: JsonNode,
    nonFilterableKeys: List<String>,
  ) {
    collectFilterKeys(filter).forEach { key ->
      if (key in nonFilterableKeys) throw S3VectorsException.INVALID_FILTER_NON_FILTERABLE_KEY
    }
  }

  private fun collectFilterKeys(node: JsonNode): Set<String> {
    val keys = mutableSetOf<String>()
    if (node.isObject) {
      for ((key, value) in node.properties()) {
        when {
          key == $$"$and" || key == $$"$or" -> {
            value.forEach { sub -> keys.addAll(collectFilterKeys(sub)) }
          }

          !key.startsWith("$") -> {
            keys.add(key)
          }

          else -> {}
        }
      }
    }
    return keys
  }

  /** Evaluates the MongoDB-style filter DSL against a vector's metadata. */
  internal fun matchesFilter(
    metadata: JsonNode?,
    filter: JsonNode,
  ): Boolean {
    if (!filter.isObject) return true
    for ((key, condition) in filter.properties()) {
      when (key) {
        $$"$and" -> {
          val allMatch = (0 until condition.size()).all { i -> matchesFilter(metadata, condition[i]) }
          if (!allMatch) return false
        }

        $$"$or" -> {
          val anyMatch = (0 until condition.size()).any { i -> matchesFilter(metadata, condition[i]) }
          if (!anyMatch) return false
        }

        else -> {
          val fieldValue = metadata?.get(key)
          if (!matchesCondition(fieldValue, condition)) return false
        }
      }
    }
    return true
  }

  private fun matchesCondition(
    fieldValue: JsonNode?,
    condition: JsonNode,
  ): Boolean {
    if (condition.isObject) {
      for ((op, operand) in condition.properties()) {
        val result =
          when (op) {
            $$"$eq" -> {
              jsonEquals(fieldValue, operand)
            }

            $$"$ne" -> {
              !jsonEquals(fieldValue, operand)
            }

            $$"$gt" -> {
              jsonCompare(fieldValue, operand) > 0
            }

            $$"$gte" -> {
              jsonCompare(fieldValue, operand) >= 0
            }

            $$"$lt" -> {
              jsonCompare(fieldValue, operand) < 0
            }

            $$"$lte" -> {
              jsonCompare(fieldValue, operand) <= 0
            }

            $$"$in" -> {
              (0 until operand.size()).any { i -> jsonEquals(fieldValue, operand[i]) }
            }

            $$"$nin" -> {
              !(0 until operand.size()).any { i -> jsonEquals(fieldValue, operand[i]) }
            }

            $$"$exists" -> {
              val shouldExist = operand.booleanValue()
              if (shouldExist) fieldValue != null && !fieldValue.isNull else fieldValue == null || fieldValue.isNull
            }

            else -> {
              true
            }
          }
        if (!result) return false
      }
      return true
    }
    return jsonEquals(fieldValue, condition)
  }

  private fun jsonEquals(
    a: JsonNode?,
    b: JsonNode,
  ): Boolean {
    if (a == null || a.isNull) return b.isNull
    if (a.isArray) return a.any { jsonEquals(it, b) }
    return a == b
  }

  private fun jsonCompare(
    a: JsonNode?,
    b: JsonNode,
  ): Int {
    if (a == null || a.isNull) return -1
    return when {
      a.isNumber && b.isNumber -> a.doubleValue().compareTo(b.doubleValue())
      a.isTextual && b.isTextual -> a.textValue().compareTo(b.textValue())
      else -> 0
    }
  }
}
