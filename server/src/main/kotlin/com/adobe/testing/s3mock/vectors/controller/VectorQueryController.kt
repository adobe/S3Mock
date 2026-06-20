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
import com.adobe.testing.s3mock.vectors.dto.QueryVectorsRequest
import com.adobe.testing.s3mock.vectors.dto.QueryVectorsResponse
import com.adobe.testing.s3mock.vectors.service.VectorQueryService
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
class VectorQueryController(
  private val vectorQueryService: VectorQueryService,
) {
  @PostMapping("/QueryVectors")
  fun queryVectors(
    @RequestBody request: QueryVectorsRequest,
  ): ResponseEntity<QueryVectorsResponse> {
    val indexArg =
      request.indexArn
        ?: request.indexName
        ?: throw S3VectorsException.validation("indexName or indexArn is required.")
    val queryVector =
      request.queryVector
        ?: throw S3VectorsException.validation("queryVector is required.")
    val response =
      vectorQueryService.queryVectors(
        bucketNameOrArn = request.vectorBucketName ?: request.indexArn,
        indexNameOrArn = indexArg,
        queryVector = queryVector,
        topK = request.topK ?: 10,
        filter = request.filter,
        returnDistance = request.returnDistance ?: false,
        returnMetadata = request.returnMetadata ?: false,
      )
    return ResponseEntity.ok(response)
  }
}
