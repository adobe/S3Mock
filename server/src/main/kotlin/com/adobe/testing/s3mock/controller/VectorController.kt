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
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
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
    @RequestBody request: CreateVectorBucketRequest,
  ): CreateVectorBucketResponse = vectorService.createVectorBucket(request)

  @PostMapping("/DeleteVectorBucket")
  fun deleteVectorBucket(
    @RequestBody request: DeleteVectorBucketRequest,
  ): ResponseEntity<Void> {
    vectorService.deleteVectorBucket(request.vectorBucketName, request.vectorBucketArn)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectorBucket")
  fun getVectorBucket(
    @RequestBody request: GetVectorBucketRequest,
  ): GetVectorBucketResponse = GetVectorBucketResponse(vectorService.getVectorBucket(request.vectorBucketName, request.vectorBucketArn))

  @PostMapping("/ListVectorBuckets")
  fun listVectorBuckets(
    @RequestBody(required = false) request: ListVectorBucketsRequest?,
  ): ListVectorBucketsResponse = vectorService.listVectorBuckets(request ?: ListVectorBucketsRequest())

  @PostMapping("/CreateIndex")
  fun createIndex(
    @RequestBody request: CreateIndexRequest,
  ): CreateIndexResponse = vectorService.createIndex(request)

  @PostMapping("/DeleteIndex")
  fun deleteIndex(
    @RequestBody request: DeleteIndexRequest,
  ): ResponseEntity<Void> {
    vectorService.deleteIndex(request.indexName, request.vectorBucketName, request.indexArn)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetIndex")
  fun getIndex(
    @RequestBody request: GetIndexRequest,
  ): GetIndexResponse = GetIndexResponse(vectorService.getIndex(request))

  @PostMapping("/ListIndexes")
  fun listIndexes(
    @RequestBody(required = false) request: ListIndexesRequest?,
  ): ListIndexesResponse = vectorService.listIndexes(request ?: ListIndexesRequest())

  @PostMapping("/PutVectors")
  fun putVectors(
    @RequestBody request: PutVectorsRequest,
  ): ResponseEntity<Void> {
    vectorService.putVectors(request.indexName, request.vectorBucketName, request.indexArn, request.vectors)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectors")
  fun getVectors(
    @RequestBody request: GetVectorsRequest,
  ): GetVectorsResponse =
    GetVectorsResponse(
      vectorService.getVectors(
        indexName = request.indexName,
        vectorBucketName = request.vectorBucketName,
        indexArn = request.indexArn,
        keys = request.keys,
        returnData = request.returnData ?: false,
        returnMetadata = request.returnMetadata ?: false,
      ),
    )

  @PostMapping("/DeleteVectors")
  fun deleteVectors(
    @RequestBody request: DeleteVectorsRequest,
  ): ResponseEntity<Void> {
    vectorService.deleteVectors(request.indexName, request.vectorBucketName, request.indexArn, request.keys)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/ListVectors")
  fun listVectors(
    @RequestBody(required = false) request: ListVectorsRequest?,
  ): ListVectorsResponse = vectorService.listVectors(request ?: ListVectorsRequest())

  @PostMapping("/QueryVectors")
  fun queryVectors(
    @RequestBody request: QueryVectorsRequest,
  ): QueryVectorsResponse = vectorService.queryVectors(request)

  @PostMapping("/PutVectorBucketPolicy")
  fun putVectorBucketPolicy(
    @RequestBody request: PutVectorBucketPolicyRequest,
  ): ResponseEntity<Void> {
    vectorService.putVectorBucketPolicy(request.policy, request.vectorBucketName, request.vectorBucketArn)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectorBucketPolicy")
  fun getVectorBucketPolicy(
    @RequestBody request: GetVectorBucketPolicyRequest,
  ): GetVectorBucketPolicyResponse = vectorService.getVectorBucketPolicy(request)

  @PostMapping("/DeleteVectorBucketPolicy")
  fun deleteVectorBucketPolicy(
    @RequestBody request: DeleteVectorBucketPolicyRequest,
  ): ResponseEntity<Void> {
    vectorService.deleteVectorBucketPolicy(request.vectorBucketName, request.vectorBucketArn)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/tags/{resourceArn:.+}")
  fun tagResource(
    @PathVariable resourceArn: String,
    @RequestBody request: TagResourceRequest,
  ): ResponseEntity<Void> {
    vectorService.tagResource(resourceArn, request.tags.orEmpty())
    return ResponseEntity.ok().build()
  }

  @GetMapping("/tags/{resourceArn:.+}")
  fun listTagsForResource(
    @PathVariable resourceArn: String,
  ): ListTagsForResourceResponse = ListTagsForResourceResponse(vectorService.listTagsForResource(resourceArn))

  @DeleteMapping("/tags/{resourceArn:.+}")
  fun untagResource(
    @PathVariable resourceArn: String,
    @RequestParam("tagKeys") tagKeys: List<String>,
  ): ResponseEntity<Void> {
    vectorService.untagResource(resourceArn, tagKeys)
    return ResponseEntity.ok().build()
  }
}
