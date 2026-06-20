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
package com.adobe.testing.s3mock.vectors.controller

import com.adobe.testing.s3mock.vectors.dto.CreateVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.CreateVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.DeleteVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketResponse
import com.adobe.testing.s3mock.vectors.dto.ListVectorBucketsRequest
import com.adobe.testing.s3mock.vectors.dto.ListVectorBucketsResponse
import com.adobe.testing.s3mock.vectors.service.VectorBucketService
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@RequestMapping(produces = [MediaType.APPLICATION_JSON_VALUE])
@RestController
class VectorBucketController(
  private val vectorBucketService: VectorBucketService,
) {
  @PostMapping("/CreateVectorBucket")
  fun createVectorBucket(
    @RequestBody request: CreateVectorBucketRequest,
  ): ResponseEntity<CreateVectorBucketResponse> {
    val name =
      request.vectorBucketName
        ?: throw com.adobe.testing.s3mock.vectors.S3VectorsException
          .validation("vectorBucketName is required.")
    val response =
      vectorBucketService.createVectorBucket(name, request.encryptionConfiguration, request.tags.orEmpty())
    return ResponseEntity.ok(response)
  }

  @PostMapping("/GetVectorBucket")
  fun getVectorBucket(
    @RequestBody request: GetVectorBucketRequest,
  ): ResponseEntity<GetVectorBucketResponse> {
    val nameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw com.adobe.testing.s3mock.vectors.S3VectorsException
          .validation("vectorBucketName or vectorBucketArn is required.")
    return ResponseEntity.ok(vectorBucketService.getVectorBucket(nameOrArn))
  }

  @PostMapping("/ListVectorBuckets")
  fun listVectorBuckets(
    @RequestBody(required = false) request: ListVectorBucketsRequest?,
  ): ResponseEntity<ListVectorBucketsResponse> {
    val r = request ?: ListVectorBucketsRequest(null, null, null)
    return ResponseEntity.ok(vectorBucketService.listVectorBuckets(r.maxResults, r.nextToken, r.prefix))
  }

  @PostMapping("/DeleteVectorBucket")
  fun deleteVectorBucket(
    @RequestBody request: DeleteVectorBucketRequest,
  ): ResponseEntity<Void> {
    val nameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw com.adobe.testing.s3mock.vectors.S3VectorsException
          .validation("vectorBucketName or vectorBucketArn is required.")
    vectorBucketService.deleteVectorBucket(nameOrArn)
    return ResponseEntity.ok().build()
  }
}
