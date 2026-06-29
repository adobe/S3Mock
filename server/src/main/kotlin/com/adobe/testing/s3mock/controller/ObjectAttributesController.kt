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
import com.adobe.testing.s3mock.dto.Checksum
import com.adobe.testing.s3mock.dto.GetObjectAttributesOutput
import com.adobe.testing.s3mock.dto.GetObjectAttributesParts
import com.adobe.testing.s3mock.dto.ObjectAttributes
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES
import com.adobe.testing.s3mock.util.AwsHttpParameters.ATTRIBUTES
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.util.versionHeader
import org.springframework.http.HttpHeaders.IF_MATCH
import org.springframework.http.HttpHeaders.IF_MODIFIED_SINCE
import org.springframework.http.HttpHeaders.IF_NONE_MATCH
import org.springframework.http.HttpHeaders.IF_UNMODIFIED_SINCE
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import java.time.Instant

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectAttributesController(
  private val bucketService: BucketService,
  private val objectService: ObjectService,
) {
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      ATTRIBUTES,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
  )
  @S3Verified(year = 2025)
  fun getObjectAttributes(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = IF_MODIFIED_SINCE, required = false) ifModifiedSince: List<Instant>?,
    @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) ifUnmodifiedSince: List<Instant>?,
    @RequestHeader(value = X_AMZ_OBJECT_ATTRIBUTES) objectAttributes: List<String>,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<GetObjectAttributesOutput> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    // this is for either an object request, or a parts request.
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match,
      noneMatch,
      ifModifiedSince,
      ifUnmodifiedSince,
      s3ObjectMetadata,
    )
    // S3Mock stores the etag with the additional quotation marks needed in the headers. This
    // response does not use eTag as a header, so it must not contain the quotation marks.
    val etag = normalizeEtag(s3ObjectMetadata.etag)!!.replace("\"", "")
    val objectSize = s3ObjectMetadata.size.toLong()
    // in object attributes, S3 returns STANDARD, in all other APIs it returns null...
    val storageClass = s3ObjectMetadata.storageClass ?: StorageClass.STANDARD
    val response =
      GetObjectAttributesOutput(
        Checksum.from(s3ObjectMetadata),
        if (objectAttributes.contains(ObjectAttributes.ETAG.toString())) {
          etag
        } else {
          null
        },
        if (objectAttributes.contains(ObjectAttributes.OBJECT_PARTS.toString()) && s3ObjectMetadata.parts != null) {
          val objectParts = s3ObjectMetadata.parts
          listOf(
            GetObjectAttributesParts(
              isTruncated = false,
              maxParts = objectParts.size,
              nextPartNumberMarker = 0,
              partNumberMarker = 0,
              partsCount = objectParts.size,
              parts = objectParts,
            ),
          )
        } else {
          null
        },
        if (objectAttributes.contains(ObjectAttributes.OBJECT_SIZE.toString())) {
          objectSize
        } else {
          null
        },
        if (objectAttributes.contains(ObjectAttributes.STORAGE_CLASS.toString())) {
          storageClass
        } else {
          null
        },
      )

    return ResponseEntity
      .ok()
      .lastModified(s3ObjectMetadata.lastModified)
      .headers { s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll) }
      .body(response)
  }
}
