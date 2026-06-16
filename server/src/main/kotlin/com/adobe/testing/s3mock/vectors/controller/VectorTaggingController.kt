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
import com.adobe.testing.s3mock.vectors.dto.ListTagsForResourceResponse
import com.adobe.testing.s3mock.vectors.dto.TagResourceRequest
import com.adobe.testing.s3mock.vectors.service.VectorArns
import com.adobe.testing.s3mock.vectors.service.VectorBucketService
import com.adobe.testing.s3mock.vectors.service.VectorIndexService
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@RequestMapping(produces = [MediaType.APPLICATION_JSON_VALUE])
@RestController
class VectorTaggingController(
  private val vectorBucketService: VectorBucketService,
  private val vectorIndexService: VectorIndexService,
) {
  @PostMapping("/tags/**")
  fun tagResource(
    request: HttpServletRequest,
    @RequestBody body: TagResourceRequest,
  ): ResponseEntity<Void> {
    val arn = extractArn(request)
    val tags = body.tags ?: emptyMap()
    if (isIndexArn(arn)) {
      vectorIndexService.tagIndex(null, arn, tags)
    } else {
      vectorBucketService.tagBucket(arn, tags)
    }
    return ResponseEntity.ok().build()
  }

  @GetMapping("/tags/**")
  fun listTagsForResource(request: HttpServletRequest): ResponseEntity<ListTagsForResourceResponse> {
    val arn = extractArn(request)
    val tags =
      if (isIndexArn(arn)) {
        vectorIndexService.getIndexTags(null, arn)
      } else {
        vectorBucketService.getBucketTags(arn)
      }
    return ResponseEntity.ok(ListTagsForResourceResponse(tags))
  }

  @DeleteMapping("/tags/**")
  fun untagResource(
    request: HttpServletRequest,
    @RequestParam tagKeys: List<String>?,
  ): ResponseEntity<Void> {
    val arn = extractArn(request)
    val keys = tagKeys ?: emptyList()
    if (isIndexArn(arn)) {
      vectorIndexService.removeIndexTags(null, arn, keys)
    } else {
      vectorBucketService.removeBucketTags(arn, keys)
    }
    return ResponseEntity.ok().build()
  }

  private fun extractArn(request: HttpServletRequest): String {
    val uri = request.requestURI
    val encoded = uri.substringAfter("/tags/")
    val arn = java.net.URLDecoder.decode(encoded, Charsets.UTF_8)
    if (!VectorArns.isArn(arn)) throw S3VectorsException.validation("Invalid resource ARN: $arn")
    return arn
  }

  private fun isIndexArn(arn: String): Boolean = arn.contains("/index/")
}
