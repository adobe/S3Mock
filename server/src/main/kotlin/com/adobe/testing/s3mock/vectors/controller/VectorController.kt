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
import com.adobe.testing.s3mock.vectors.dto.DeleteVectorsRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorsRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorsResponse
import com.adobe.testing.s3mock.vectors.dto.ListVectorsRequest
import com.adobe.testing.s3mock.vectors.dto.ListVectorsResponse
import com.adobe.testing.s3mock.vectors.dto.PutVectorsRequest
import com.adobe.testing.s3mock.vectors.service.VectorService
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
class VectorController(
  private val vectorService: VectorService,
) {
  @PostMapping("/PutVectors")
  fun putVectors(
    @RequestBody request: PutVectorsRequest,
  ): ResponseEntity<Void> {
    val indexArg =
      request.indexArn
        ?: request.indexName
        ?: throw S3VectorsException.validation("indexName or indexArn is required.")
    vectorService.putVectors(
      bucketNameOrArn = request.vectorBucketName ?: request.indexArn,
      indexNameOrArn = indexArg,
      vectors = request.vectors ?: emptyList(),
    )
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectors")
  fun getVectors(
    @RequestBody request: GetVectorsRequest,
  ): ResponseEntity<GetVectorsResponse> {
    val indexArg =
      request.indexArn
        ?: request.indexName
        ?: throw S3VectorsException.validation("indexName or indexArn is required.")
    val response =
      vectorService.getVectors(
        bucketNameOrArn = request.vectorBucketName ?: request.indexArn,
        indexNameOrArn = indexArg,
        keys = request.keys ?: emptyList(),
        returnData = request.returnData ?: false,
        returnMetadata = request.returnMetadata ?: false,
      )
    return ResponseEntity.ok(response)
  }

  @PostMapping("/ListVectors")
  fun listVectors(
    @RequestBody request: ListVectorsRequest,
  ): ResponseEntity<ListVectorsResponse> {
    val indexArg =
      request.indexArn
        ?: request.indexName
        ?: throw S3VectorsException.validation("indexName or indexArn is required.")
    val response =
      vectorService.listVectors(
        bucketNameOrArn = request.vectorBucketName ?: request.indexArn,
        indexNameOrArn = indexArg,
        maxResults = request.maxResults,
        nextToken = request.nextToken,
        returnData = request.returnData ?: false,
        returnMetadata = request.returnMetadata ?: false,
        segmentCount = request.segmentCount,
        segmentIndex = request.segmentIndex,
      )
    return ResponseEntity.ok(response)
  }

  @PostMapping("/DeleteVectors")
  fun deleteVectors(
    @RequestBody request: DeleteVectorsRequest,
  ): ResponseEntity<Void> {
    val indexArg =
      request.indexArn
        ?: request.indexName
        ?: throw S3VectorsException.validation("indexName or indexArn is required.")
    vectorService.deleteVectors(
      bucketNameOrArn = request.vectorBucketName ?: request.indexArn,
      indexNameOrArn = indexArg,
      keys = request.keys ?: emptyList(),
    )
    return ResponseEntity.ok().build()
  }
}
