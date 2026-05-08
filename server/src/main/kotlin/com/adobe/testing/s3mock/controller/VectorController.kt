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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.s3mock.service.VectorService
import com.adobe.testing.s3mock.store.VectorRecord
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.JsonNode

@RestController
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class VectorController(
  private val vectorService: VectorService,
) {
  @PostMapping("/CreateVectorBucket")
  fun createVectorBucket(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.createVectorBucket(
        vectorBucketName = requiredText(body, "VectorBucketName"),
        encryptionConfiguration = optionalObject(body, "EncryptionConfiguration"),
        tags = parseTags(body),
      ),
    )

  @PostMapping("/DeleteVectorBucket")
  fun deleteVectorBucket(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.deleteVectorBucket(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      vectorBucketArn = optionalText(body, "VectorBucketArn"),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/GetVectorBucket")
  fun getVectorBucket(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.getVectorBucket(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        vectorBucketArn = optionalText(body, "VectorBucketArn"),
      ),
    )

  @PostMapping("/ListVectorBuckets")
  fun listVectorBuckets(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.listVectorBuckets(
        prefix = optionalText(body, "Prefix"),
        maxResults = optionalInt(body, "MaxResults"),
        nextToken = optionalText(body, "NextToken"),
      ),
    )

  @PostMapping("/CreateIndex")
  fun createIndex(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.createIndex(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        vectorBucketArn = optionalText(body, "VectorBucketArn"),
        indexName = requiredText(body, "IndexName"),
        dataType = requiredText(body, "DataType"),
        dimension = requiredInt(body, "Dimension"),
        distanceMetric = requiredText(body, "DistanceMetric"),
        metadataConfiguration = optionalObject(body, "MetadataConfiguration"),
        encryptionConfiguration = optionalObject(body, "EncryptionConfiguration"),
        tags = parseTags(body),
      ),
    )

  @PostMapping("/DeleteIndex")
  fun deleteIndex(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.deleteIndex(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      indexName = optionalText(body, "IndexName"),
      indexArn = optionalText(body, "IndexArn"),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/GetIndex")
  fun getIndex(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.getIndex(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        indexName = optionalText(body, "IndexName"),
        indexArn = optionalText(body, "IndexArn"),
      ),
    )

  @PostMapping("/ListIndexes")
  fun listIndexes(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.listIndexes(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        vectorBucketArn = optionalText(body, "VectorBucketArn"),
        maxResults = optionalInt(body, "MaxResults"),
        nextToken = optionalText(body, "NextToken"),
        prefix = optionalText(body, "Prefix"),
      ),
    )

  @PostMapping("/PutVectors")
  fun putVectors(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.putVectors(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      indexName = optionalText(body, "IndexName"),
      indexArn = optionalText(body, "IndexArn"),
      vectors = requiredVectors(body),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/GetVectors")
  fun getVectors(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.getVectors(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        indexName = optionalText(body, "IndexName"),
        indexArn = optionalText(body, "IndexArn"),
        keys = requiredTexts(body, "Keys"),
        returnData = optionalBoolean(body, "ReturnData"),
        returnMetadata = optionalBoolean(body, "ReturnMetadata"),
      ),
    )

  @PostMapping("/DeleteVectors")
  fun deleteVectors(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.deleteVectors(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      indexName = optionalText(body, "IndexName"),
      indexArn = optionalText(body, "IndexArn"),
      keys = requiredTexts(body, "Keys"),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/ListVectors")
  fun listVectors(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.listVectors(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        indexName = optionalText(body, "IndexName"),
        indexArn = optionalText(body, "IndexArn"),
        maxResults = optionalInt(body, "MaxResults"),
        nextToken = optionalText(body, "NextToken"),
        returnData = optionalBoolean(body, "ReturnData"),
        returnMetadata = optionalBoolean(body, "ReturnMetadata"),
      ),
    )

  @PostMapping("/QueryVectors")
  fun queryVectors(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.queryVectors(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        indexName = optionalText(body, "IndexName"),
        indexArn = optionalText(body, "IndexArn"),
        topK = requiredInt(body, "TopK"),
        queryVector = requiredVectorData(body["QueryVector"]),
        returnMetadata = optionalBoolean(body, "ReturnMetadata"),
        returnDistance = optionalBoolean(body, "ReturnDistance"),
      ),
    )

  @PostMapping("/PutVectorBucketPolicy")
  fun putVectorBucketPolicy(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.putVectorBucketPolicy(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      vectorBucketArn = optionalText(body, "VectorBucketArn"),
      policy = requiredText(body, "Policy"),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/GetVectorBucketPolicy")
  fun getVectorBucketPolicy(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> =
    ResponseEntity.ok(
      vectorService.getVectorBucketPolicy(
        vectorBucketName = optionalText(body, "VectorBucketName"),
        vectorBucketArn = optionalText(body, "VectorBucketArn"),
      ),
    )

  @PostMapping("/DeleteVectorBucketPolicy")
  fun deleteVectorBucketPolicy(
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.deleteVectorBucketPolicy(
      vectorBucketName = optionalText(body, "VectorBucketName"),
      vectorBucketArn = optionalText(body, "VectorBucketArn"),
    )
    return ResponseEntity.ok(emptyMap())
  }

  @PostMapping("/tags/{*resourceArn}")
  fun tagResource(
    @PathVariable resourceArn: String,
    @RequestBody body: JsonNode,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.tagResource(resourceArn, parseTags(body))
    return ResponseEntity.ok(emptyMap())
  }

  @DeleteMapping("/tags/{*resourceArn}")
  fun untagResource(
    @PathVariable resourceArn: String,
    @RequestParam("tagKeys") tagKeys: List<String>,
  ): ResponseEntity<Map<String, Any?>> {
    vectorService.untagResource(resourceArn, tagKeys)
    return ResponseEntity.ok(emptyMap())
  }

  @GetMapping("/tags/{*resourceArn}")
  fun listTagsForResource(
    @PathVariable resourceArn: String,
  ): ResponseEntity<Map<String, Any?>> = ResponseEntity.ok(vectorService.listTagsForResource(resourceArn))

  private fun requiredText(
    body: JsonNode,
    field: String,
  ): String = optionalText(body, field) ?: throw VectorApiException.validation("Missing required field: $field")

  private fun optionalText(
    body: JsonNode,
    field: String,
  ): String? = body[field]?.takeUnless { it.isNull }?.asText()

  private fun requiredInt(
    body: JsonNode,
    field: String,
  ): Int = body[field]?.takeUnless { it.isNull }?.asInt() ?: throw VectorApiException.validation("Missing required field: $field")

  private fun optionalInt(
    body: JsonNode,
    field: String,
  ): Int? = body[field]?.takeUnless { it.isNull }?.asInt()

  private fun optionalBoolean(
    body: JsonNode,
    field: String,
  ): Boolean = body[field]?.takeUnless { it.isNull }?.asBoolean() ?: false

  private fun optionalObject(
    body: JsonNode,
    field: String,
  ): Map<String, Any?>? {
    val node = body[field] ?: return null
    if (!node.isObject) return null
    return node.fields().asSequence().associate { it.key to jsonValue(it.value) }
  }

  private fun requiredTexts(
    body: JsonNode,
    field: String,
  ): List<String> {
    val node = body[field]
    if (node == null || !node.isArray) throw VectorApiException.validation("Missing required field: $field")
    return node.map { it.asText() }
  }

  private fun parseTags(body: JsonNode): Map<String, String> {
    val node = body["Tags"] ?: return emptyMap()
    if (!node.isObject) return emptyMap()
    return node.fields().asSequence().associate { it.key to it.value.asText() }
  }

  private fun requiredVectors(body: JsonNode): List<VectorRecord> {
    val vectors = body["Vectors"] ?: throw VectorApiException.validation("Missing required field: Vectors")
    if (!vectors.isArray) throw VectorApiException.validation("Vectors must be an array.")
    return vectors.map { vectorNode ->
      VectorRecord(
        key = requiredText(vectorNode, "Key"),
        data = requiredVectorData(vectorNode["Data"]),
        metadata = optionalStringMap(vectorNode, "Metadata"),
      )
    }
  }

  private fun requiredVectorData(node: JsonNode?): List<Double> {
    val float32 = node?.get("Float32") ?: throw VectorApiException.validation("Missing required field: Data.Float32")
    if (!float32.isArray) throw VectorApiException.validation("Data.Float32 must be an array.")
    return float32.map { it.asDouble() }
  }

  private fun optionalStringMap(
    body: JsonNode,
    field: String,
  ): Map<String, String>? {
    val node = body[field] ?: return null
    if (!node.isObject) return null
    return node.fields().asSequence().associate { it.key to it.value.asText() }
  }

  private fun jsonValue(value: JsonNode): Any? =
    when {
      value.isTextual -> value.asText()
      value.isInt -> value.intValue()
      value.isLong -> value.longValue()
      value.isFloat || value.isDouble -> value.doubleValue()
      value.isBoolean -> value.booleanValue()
      value.isArray -> value.map(::jsonValue)
      value.isObject -> value.fields().asSequence().associate { it.key to jsonValue(it.value) }
      else -> null
    }
}

class VectorApiException(
  val status: HttpStatus,
  val code: String,
  override val message: String,
) : RuntimeException(message) {
  companion object {
    fun notFound(message: String): VectorApiException = VectorApiException(HttpStatus.NOT_FOUND, "NotFoundException", message)

    fun validation(message: String): VectorApiException = VectorApiException(HttpStatus.BAD_REQUEST, "ValidationException", message)
  }
}

@ControllerAdvice
class VectorApiExceptionHandler {
  @ExceptionHandler(VectorApiException::class)
  fun handle(exception: VectorApiException): ResponseEntity<Map<String, Any?>> =
    ResponseEntity
      .status(exception.status)
      .header("x-amzn-errortype", exception.code)
      .body(
        mapOf(
          "code" to exception.code,
          "message" to exception.message,
        ),
      )
}
