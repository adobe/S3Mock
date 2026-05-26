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

import com.adobe.testing.s3mock.dto.CreateIndexRequest
import com.adobe.testing.s3mock.dto.CreateIndexResponse
import com.adobe.testing.s3mock.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.dto.DeleteIndexRequest
import com.adobe.testing.s3mock.dto.DeleteVectorBucketPolicyRequest
import com.adobe.testing.s3mock.dto.DeleteVectorBucketRequest
import com.adobe.testing.s3mock.dto.DeleteVectorsRequest
import com.adobe.testing.s3mock.dto.GetIndexRequest
import com.adobe.testing.s3mock.dto.GetIndexResponse
import com.adobe.testing.s3mock.dto.GetVectorBucketPolicyRequest
import com.adobe.testing.s3mock.dto.GetVectorBucketPolicyResponse
import com.adobe.testing.s3mock.dto.GetVectorBucketRequest
import com.adobe.testing.s3mock.dto.GetVectorBucketResponse
import com.adobe.testing.s3mock.dto.GetVectorsRequest
import com.adobe.testing.s3mock.dto.GetVectorsResponse
import com.adobe.testing.s3mock.dto.ListIndexesRequest
import com.adobe.testing.s3mock.dto.ListIndexesResponse
import com.adobe.testing.s3mock.dto.ListTagsForResourceResponse
import com.adobe.testing.s3mock.dto.ListVectorBucketsRequest
import com.adobe.testing.s3mock.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.dto.ListVectorsRequest
import com.adobe.testing.s3mock.dto.ListVectorsResponse
import com.adobe.testing.s3mock.dto.PutVectorBucketPolicyRequest
import com.adobe.testing.s3mock.dto.PutVectorsRequest
import com.adobe.testing.s3mock.dto.QueryVectorsRequest
import com.adobe.testing.s3mock.dto.QueryVectorsResponse
import com.adobe.testing.s3mock.dto.TagResourceRequest
import com.adobe.testing.s3mock.service.VectorService
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

@RestController
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class VectorController(
  private val vectorService: VectorService,
) {
  @PostMapping("/CreateVectorBucket")
  fun createVectorBucket(
    @RequestBody body: CreateVectorBucketRequest,
  ): ResponseEntity<CreateVectorBucketResponse> =
    ResponseEntity.ok(
      vectorService.createVectorBucket(
        vectorBucketName = body.vectorBucketName ?: throw VectorApiException.validation("Missing required field: VectorBucketName"),
        encryptionConfiguration = body.encryptionConfiguration,
        tags = body.tags?.values ?: emptyMap(),
      ),
    )

  @PostMapping("/DeleteVectorBucket")
  fun deleteVectorBucket(
    @RequestBody body: DeleteVectorBucketRequest,
  ): ResponseEntity<Unit> {
    vectorService.deleteVectorBucket(
      vectorBucketName = body.vectorBucketName,
      vectorBucketArn = body.vectorBucketArn,
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectorBucket")
  fun getVectorBucket(
    @RequestBody body: GetVectorBucketRequest,
  ): ResponseEntity<GetVectorBucketResponse> =
    ResponseEntity.ok(
      vectorService.getVectorBucket(
        vectorBucketName = body.vectorBucketName,
        vectorBucketArn = body.vectorBucketArn,
      ),
    )

  @PostMapping("/ListVectorBuckets")
  fun listVectorBuckets(
    @RequestBody body: ListVectorBucketsRequest,
  ): ResponseEntity<ListVectorBucketsResponse> =
    ResponseEntity.ok(
      vectorService.listVectorBuckets(
        prefix = body.prefix,
        maxResults = body.maxResults,
        nextToken = body.nextToken,
      ),
    )

  @PostMapping("/CreateIndex")
  fun createIndex(
    @RequestBody body: CreateIndexRequest,
  ): ResponseEntity<CreateIndexResponse> =
    ResponseEntity.ok(
      vectorService.createIndex(
        vectorBucketName = body.vectorBucketName,
        vectorBucketArn = body.vectorBucketArn,
        indexName = body.indexName ?: throw VectorApiException.validation("Missing required field: IndexName"),
        dataType = body.dataType ?: throw VectorApiException.validation("Missing required field: DataType"),
        dimension = body.dimension ?: throw VectorApiException.validation("Missing required field: Dimension"),
        distanceMetric = body.distanceMetric ?: throw VectorApiException.validation("Missing required field: DistanceMetric"),
        metadataConfiguration = body.metadataConfiguration,
        encryptionConfiguration = body.encryptionConfiguration,
        tags = body.tags?.values ?: emptyMap(),
      ),
    )

  @PostMapping("/DeleteIndex")
  fun deleteIndex(
    @RequestBody body: DeleteIndexRequest,
  ): ResponseEntity<Unit> {
    vectorService.deleteIndex(
      vectorBucketName = body.vectorBucketName,
      indexName = body.indexName,
      indexArn = body.indexArn,
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetIndex")
  fun getIndex(
    @RequestBody body: GetIndexRequest,
  ): ResponseEntity<GetIndexResponse> =
    ResponseEntity.ok(
      vectorService.getIndex(
        vectorBucketName = body.vectorBucketName,
        indexName = body.indexName,
        indexArn = body.indexArn,
      ),
    )

  @PostMapping("/ListIndexes")
  fun listIndexes(
    @RequestBody body: ListIndexesRequest,
  ): ResponseEntity<ListIndexesResponse> =
    ResponseEntity.ok(
      vectorService.listIndexes(
        vectorBucketName = body.vectorBucketName,
        vectorBucketArn = body.vectorBucketArn,
        maxResults = body.maxResults,
        nextToken = body.nextToken,
        prefix = body.prefix,
      ),
    )

  @PostMapping("/PutVectors")
  fun putVectors(
    @RequestBody body: PutVectorsRequest,
  ): ResponseEntity<Unit> {
    vectorService.putVectors(
      vectorBucketName = body.vectorBucketName,
      indexName = body.indexName,
      indexArn = body.indexArn,
      vectors = body.vectors ?: throw VectorApiException.validation("Missing required field: Vectors"),
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectors")
  fun getVectors(
    @RequestBody body: GetVectorsRequest,
  ): ResponseEntity<GetVectorsResponse> =
    ResponseEntity.ok(
      vectorService.getVectors(
        vectorBucketName = body.vectorBucketName,
        indexName = body.indexName,
        indexArn = body.indexArn,
        keys = body.keys ?: throw VectorApiException.validation("Missing required field: Keys"),
        returnData = body.returnData ?: false,
        returnMetadata = body.returnMetadata ?: false,
      ),
    )

  @PostMapping("/DeleteVectors")
  fun deleteVectors(
    @RequestBody body: DeleteVectorsRequest,
  ): ResponseEntity<Unit> {
    vectorService.deleteVectors(
      vectorBucketName = body.vectorBucketName,
      indexName = body.indexName,
      indexArn = body.indexArn,
      keys = body.keys ?: throw VectorApiException.validation("Missing required field: Keys"),
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/ListVectors")
  fun listVectors(
    @RequestBody body: ListVectorsRequest,
  ): ResponseEntity<ListVectorsResponse> =
    ResponseEntity.ok(
      vectorService.listVectors(
        vectorBucketName = body.vectorBucketName,
        indexName = body.indexName,
        indexArn = body.indexArn,
        maxResults = body.maxResults,
        nextToken = body.nextToken,
        returnData = body.returnData ?: false,
        returnMetadata = body.returnMetadata ?: false,
      ),
    )

  @PostMapping("/QueryVectors")
  fun queryVectors(
    @RequestBody body: QueryVectorsRequest,
  ): ResponseEntity<QueryVectorsResponse> =
    ResponseEntity.ok(
      vectorService.queryVectors(
        vectorBucketName = body.vectorBucketName,
        indexName = body.indexName,
        indexArn = body.indexArn,
        topK = body.topK ?: throw VectorApiException.validation("Missing required field: TopK"),
        queryVector = body.queryVector ?: throw VectorApiException.validation("Missing required field: QueryVector"),
        returnMetadata = body.returnMetadata ?: false,
        returnDistance = body.returnDistance ?: false,
      ),
    )

  @PostMapping("/PutVectorBucketPolicy")
  fun putVectorBucketPolicy(
    @RequestBody body: PutVectorBucketPolicyRequest,
  ): ResponseEntity<Unit> {
    vectorService.putVectorBucketPolicy(
      vectorBucketName = body.vectorBucketName,
      vectorBucketArn = body.vectorBucketArn,
      policy = body.policy ?: throw VectorApiException.validation("Missing required field: Policy"),
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectorBucketPolicy")
  fun getVectorBucketPolicy(
    @RequestBody body: GetVectorBucketPolicyRequest,
  ): ResponseEntity<GetVectorBucketPolicyResponse> =
    ResponseEntity.ok(
      vectorService.getVectorBucketPolicy(
        vectorBucketName = body.vectorBucketName,
        vectorBucketArn = body.vectorBucketArn,
      ),
    )

  @PostMapping("/DeleteVectorBucketPolicy")
  fun deleteVectorBucketPolicy(
    @RequestBody body: DeleteVectorBucketPolicyRequest,
  ): ResponseEntity<Unit> {
    vectorService.deleteVectorBucketPolicy(
      vectorBucketName = body.vectorBucketName,
      vectorBucketArn = body.vectorBucketArn,
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/tags/{*resourceArn}")
  fun tagResource(
    @PathVariable resourceArn: String,
    @RequestBody body: TagResourceRequest,
  ): ResponseEntity<Unit> {
    vectorService.tagResource(resourceArn, body.tags?.values ?: emptyMap())
    return ResponseEntity.ok().build()
  }

  @DeleteMapping("/tags/{*resourceArn}")
  fun untagResource(
    @PathVariable resourceArn: String,
    @RequestParam("tagKeys") tagKeys: List<String>,
  ): ResponseEntity<Unit> {
    vectorService.untagResource(resourceArn, tagKeys)
    return ResponseEntity.ok().build()
  }

  @GetMapping("/tags/{*resourceArn}")
  fun listTagsForResource(
    @PathVariable resourceArn: String,
  ): ResponseEntity<ListTagsForResourceResponse> = ResponseEntity.ok(vectorService.listTagsForResource(resourceArn))
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
