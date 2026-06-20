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
import com.adobe.testing.s3mock.vectors.dto.DeleteVectorBucketPolicyRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketPolicyRequest
import com.adobe.testing.s3mock.vectors.dto.GetVectorBucketPolicyResponse
import com.adobe.testing.s3mock.vectors.dto.PutVectorBucketPolicyRequest
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
class VectorPolicyController(
  private val vectorBucketService: VectorBucketService,
) {
  @PostMapping("/PutVectorBucketPolicy")
  fun putVectorBucketPolicy(
    @RequestBody request: PutVectorBucketPolicyRequest,
  ): ResponseEntity<Void> {
    val nameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw S3VectorsException.validation("vectorBucketName or vectorBucketArn is required.")
    val policy = request.policy ?: throw S3VectorsException.validation("policy is required.")
    vectorBucketService.putPolicy(nameOrArn, policy)
    return ResponseEntity.ok().build()
  }

  @PostMapping("/GetVectorBucketPolicy")
  fun getVectorBucketPolicy(
    @RequestBody request: GetVectorBucketPolicyRequest,
  ): ResponseEntity<GetVectorBucketPolicyResponse> {
    val nameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw S3VectorsException.validation("vectorBucketName or vectorBucketArn is required.")
    val policy =
      vectorBucketService.getPolicy(nameOrArn)
        ?: throw S3VectorsException.BUCKET_POLICY_NOT_FOUND
    return ResponseEntity.ok(GetVectorBucketPolicyResponse(policy))
  }

  @PostMapping("/DeleteVectorBucketPolicy")
  fun deleteVectorBucketPolicy(
    @RequestBody request: DeleteVectorBucketPolicyRequest,
  ): ResponseEntity<Void> {
    val nameOrArn =
      request.vectorBucketName ?: request.vectorBucketArn
        ?: throw S3VectorsException.validation("vectorBucketName or vectorBucketArn is required.")
    vectorBucketService.deletePolicy(nameOrArn)
    return ResponseEntity.ok().build()
  }
}
