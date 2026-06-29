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

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.TagSet
import com.adobe.testing.s3mock.dto.Tagging
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.versionHeader
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectTaggingController(
  private val bucketService: BucketService,
  private val objectService: ObjectService,
) {
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      TAGGING,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_XML_VALUE + ";charset=UTF-8",
    ],
  )
  @S3Verified(year = 2025)
  fun getObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<Tagging> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)

    val tagging =
      s3ObjectMetadata.tags
        ?.takeIf { it.isNotEmpty() }
        ?.let { Tagging(TagSet(it)) }

    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .lastModified(s3ObjectMetadata.lastModified)
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .body(tagging)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      TAGGING,
    ],
  )
  @S3Verified(year = 2025)
  fun putObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody body: Tagging,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectTags(body.tagSet.tags)
    objectService.setTags(bucketName, key.key, versionId, body.tagSet.tags)
    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .lastModified(s3ObjectMetadata.lastModified)
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html).
   */
  @DeleteMapping(
    value = ["/{bucketName:.+}/{*key}"],
    params = [
      TAGGING,
    ],
  )
  @S3Verified(year = 2025)
  fun deleteObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.setTags(bucketName, key.key, versionId, null)
    return ResponseEntity
      .noContent()
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .build()
  }
}
