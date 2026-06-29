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
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.ObjectCannedACL
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_ACL
import com.adobe.testing.s3mock.util.AwsHttpParameters.ACL
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.CannedAclUtil.policyForCannedAcl
import com.adobe.testing.s3mock.util.versionHeader
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectAclController(
  private val bucketService: BucketService,
  private val objectService: ObjectService,
) {
  /**
   * Adds an ACL to an object, either from an [AccessControlPolicy] request body or from a
   * canned ACL header.
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      ACL,
    ],
  )
  @S3Verified(year = 2025)
  fun putObjectAcl(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_ACL, required = false) cannedAcl: ObjectCannedACL?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody(required = false) body: AccessControlPolicy?,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    val policy =
      body
        ?: if (cannedAcl != null) {
          policyForCannedAcl(cannedAcl)
        } else {
          return ResponseEntity.badRequest().build()
        }
    objectService.setAcl(bucketName, key.key, versionId, policy)
    return ResponseEntity
      .ok()
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .build()
  }

  /**
   * Gets ACL of an object.
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html)
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      ACL,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
  )
  @S3Verified(year = 2025)
  fun getObjectAcl(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<AccessControlPolicy> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    val acl = objectService.getAcl(bucketName, key.key, versionId)
    return ResponseEntity
      .ok()
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .body(acl)
  }
}
