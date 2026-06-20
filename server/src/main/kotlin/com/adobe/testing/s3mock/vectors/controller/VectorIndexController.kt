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

import com.adobe.testing.s3mock.vectors.S3VectorsException
import com.adobe.testing.s3mock.vectors.dto.CreateIndexRequest
import com.adobe.testing.s3mock.vectors.dto.CreateIndexResponse
import com.adobe.testing.s3mock.vectors.dto.DeleteIndexRequest
import com.adobe.testing.s3mock.vectors.dto.GetIndexRequest
import com.adobe.testing.s3mock.vectors.dto.GetIndexResponse
import com.adobe.testing.s3mock.vectors.dto.ListIndexesRequest
import com.adobe.testing.s3mock.vectors.dto.ListIndexesResponse
import com.adobe.testing.s3mock.vectors.service.VectorIndexService
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
class VectorIndexController(
  private val vectorIndexService: VectorIndexService,
) {
  @PostMapping("/CreateIndex")
  fun createIndex(
    @RequestBody request: CreateIndexRequest,
  ): ResponseEntity<CreateIndexResponse> {
    val bucketNameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw S3VectorsException.validation("vectorBucketName or vectorBucketArn is required.")
    val response =
      vectorIndexService.createIndex(
        bucketNameOrArn = bucketNameOrArn,
        indexName = request.indexName ?: throw S3VectorsException.validation("indexName is required."),
        dataType = request.dataType ?: "float32",
        dimension = request.dimension ?: throw S3VectorsException.validation("dimension is required."),
        distanceMetric = request.distanceMetric ?: throw S3VectorsException.validation("distanceMetric is required."),
        metadataConfiguration = request.metadataConfiguration,
        encryption = request.encryptionConfiguration,
        tags = request.tags.orEmpty(),
      )
    return ResponseEntity.ok(response)
  }

  @PostMapping("/GetIndex")
  fun getIndex(
    @RequestBody request: GetIndexRequest,
  ): ResponseEntity<GetIndexResponse> {
    val (bucketArg, indexArg) =
      when {
        request.indexArn != null -> null to request.indexArn
        request.indexName != null -> (request.vectorBucketName) to request.indexName
        else -> throw S3VectorsException.validation("indexName or indexArn is required.")
      }
    return ResponseEntity.ok(vectorIndexService.getIndex(bucketArg, indexArg))
  }

  @PostMapping("/ListIndexes")
  fun listIndexes(
    @RequestBody(required = false) request: ListIndexesRequest?,
  ): ResponseEntity<ListIndexesResponse> {
    val bucketNameOrArn =
      request?.vectorBucketName ?: request?.vectorBucketArn
        ?: throw S3VectorsException.validation("vectorBucketName or vectorBucketArn is required.")
    return ResponseEntity.ok(
      vectorIndexService.listIndexes(
        bucketNameOrArn = bucketNameOrArn,
        prefix = request?.prefix,
        maxResults = request?.maxResults,
        nextToken = request?.nextToken,
      ),
    )
  }

  @PostMapping("/DeleteIndex")
  fun deleteIndex(
    @RequestBody request: DeleteIndexRequest,
  ): ResponseEntity<Void> {
    val (bucketArg, indexArg) =
      when {
        request.indexArn != null -> null to request.indexArn
        request.indexName != null -> (request.vectorBucketName) to request.indexName
        else -> throw S3VectorsException.validation("indexName or indexArn is required.")
      }
    vectorIndexService.deleteIndex(bucketArg, indexArg)
    return ResponseEntity.ok().build()
  }
}
