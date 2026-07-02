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
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpParameters.RETENTION
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.versionHeader
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectRetentionController(
  private val bucketService: BucketService,
  private val objectService: ObjectService,
) {
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html).
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      RETENTION,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
  )
  fun getObjectRetention(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<Retention> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)
    val s3ObjectMetadata = objectService.verifyRetentionExists(bucketName, key.key, versionId)

    return ResponseEntity
      .ok()
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .body(s3ObjectMetadata.retention)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      RETENTION,
    ],
  )
  @S3Verified(year = 2025)
  fun putObjectRetention(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody body: Retention,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyRetention(body)
    objectService.setRetention(bucketName, key.key, versionId, body)
    return ResponseEntity
      .ok()
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .build()
  }
}
