/*
 *  Copyright 2017-2025 Adobe.
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
import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.Checksum
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumMode
import com.adobe.testing.s3mock.dto.CopyObjectResult
import com.adobe.testing.s3mock.dto.CopySource
import com.adobe.testing.s3mock.dto.Delete
import com.adobe.testing.s3mock.dto.DeleteResult
import com.adobe.testing.s3mock.dto.GetObjectAttributesOutput
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.ObjectAttributes
import com.adobe.testing.s3mock.dto.ObjectCannedACL
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.TagSet
import com.adobe.testing.s3mock.dto.Tagging
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective
import com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_ACL
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_MODE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_VERSION_ID
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_LAST_MODIFIED_TIME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_SIZE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_SIZE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_VERSION_ID
import com.adobe.testing.s3mock.util.AwsHttpParameters.ACL
import com.adobe.testing.s3mock.util.AwsHttpParameters.ATTRIBUTES
import com.adobe.testing.s3mock.util.AwsHttpParameters.DELETE
import com.adobe.testing.s3mock.util.AwsHttpParameters.FILE
import com.adobe.testing.s3mock.util.AwsHttpParameters.KEY
import com.adobe.testing.s3mock.util.AwsHttpParameters.LEGAL_HOLD
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_ACL
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_ATTRIBUTES
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_DELETE
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LEGAL_HOLD
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_RETENTION
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_TAGGING
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOAD_ID
import com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER
import com.adobe.testing.s3mock.util.AwsHttpParameters.RETENTION
import com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.BoundedInputStream
import com.adobe.testing.s3mock.util.CannedAclUtil.policyForCannedAcl
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromHeader
import com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk
import com.adobe.testing.s3mock.util.HeaderUtil.checksumFrom
import com.adobe.testing.s3mock.util.HeaderUtil.checksumHeaderFrom
import com.adobe.testing.s3mock.util.HeaderUtil.encryptionHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.mediaTypeFrom
import com.adobe.testing.s3mock.util.HeaderUtil.overrideHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.storageClassHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.storeHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.userMetadataFrom
import com.adobe.testing.s3mock.util.HeaderUtil.userMetadataHeadersFrom
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpHeaders.ACCEPT_RANGES
import org.springframework.http.HttpHeaders.CONTENT_RANGE
import org.springframework.http.HttpHeaders.CONTENT_TYPE
import org.springframework.http.HttpHeaders.IF_MATCH
import org.springframework.http.HttpHeaders.IF_MODIFIED_SINCE
import org.springframework.http.HttpHeaders.IF_NONE_MATCH
import org.springframework.http.HttpHeaders.IF_UNMODIFIED_SINCE
import org.springframework.http.HttpRange
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatus.PARTIAL_CONTENT
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RequestPart
import org.springframework.web.multipart.MultipartFile
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.time.Instant
import kotlin.math.min

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectController(private val bucketService: BucketService, private val objectService: ObjectService) {
  // ===============================================================================================
  // /{bucketName:.+}
  // ===============================================================================================
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html).
   */
  @PostMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      DELETE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun deleteObjects(
    @PathVariable bucketName: String,
    @RequestBody body: Delete
  ): ResponseEntity<DeleteResult> {
    bucketService.verifyBucketExists(bucketName)
    return ResponseEntity.ok(objectService.deleteObjects(bucketName, body))
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html).
   * Does not support all parameters listed in the API reference.
   */
  @PostMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      NOT_DELETE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ],
    consumes = [
      MediaType.MULTIPART_FORM_DATA_VALUE
    ]
  )
  @Throws(IOException::class)
  fun postObject(
    @PathVariable bucketName: String,
    @RequestParam(value = KEY) key: ObjectKey,
    @RequestParam(value = TAGGING, required = false) tags: List<Tag>?,
    @RequestParam(value = CONTENT_TYPE, required = false) contentType: String?,
    @RequestParam(value = CONTENT_MD5, required = false) contentMd5: String?,
    @RequestParam(value = X_AMZ_STORAGE_CLASS, required = false, defaultValue = "STANDARD") storageClass: StorageClass,
    @RequestPart(FILE) file: MultipartFile
  ): ResponseEntity<Void> {
    val (tempFile, _) = objectService.toTempFile(file.inputStream)

    val bucket = bucketService.verifyBucketExists(bucketName)
    objectService.verifyMd5(tempFile, contentMd5)

    val s3ObjectMetadata = objectService.putS3Object(
      bucketName = bucketName,
      key = key.key,
      contentType = mediaTypeFrom(contentType).toString(),
      storeHeaders = emptyMap(),
      path = tempFile,
      userMetadata = emptyMap(),
      encryptionHeaders = emptyMap(),
      tags = tags,
      checksumAlgorithm = null,
      checksum = null,
      owner = Owner.DEFAULT_OWNER,
      storageClass = storageClass
    )

    runCatching { tempFile.toFile().deleteRecursively() }

    return ResponseEntity
      .ok()
      .headers { it.setAll(checksumHeaderFrom(s3ObjectMetadata)) }
      .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
      .lastModified(s3ObjectMetadata.lastModified)
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build()
  }

  // ===============================================================================================
  // /{bucketName:.+}/{*key}
  // ===============================================================================================
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
   */
  @RequestMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    method = [
      RequestMethod.HEAD
    ]
  )
  @S3Verified(year = 2025)
  fun headObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = IF_MODIFIED_SINCE, required = false) ifModifiedSince: List<Instant>?,
    @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) ifUnmodifiedSince: List<Instant>?,
    @RequestHeader(value = RANGE, required = false) range: HttpRange?,
    @RequestParam(value = PART_NUMBER, required = false) partNumber: String?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestParam queryParams: Map<String, String>
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match, noneMatch,
      ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata
    )

    return ResponseEntity.ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(s3ObjectMetadata.size.toLong())
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .headers {
        if (s3ObjectMetadata.storeHeaders != null) {
          it.setAll(s3ObjectMetadata.storeHeaders)
        }
      }
      .headers { it.setAll(userMetadataHeadersFrom(s3ObjectMetadata)) }
      .headers {
        if (s3ObjectMetadata.encryptionHeaders != null) {
          it.setAll(s3ObjectMetadata.encryptionHeaders)
        }
      }
      .headers { it.setAll(checksumHeaderFrom(s3ObjectMetadata)) }
      .headers { it.setAll(storageClassHeadersFrom(s3ObjectMetadata)) }
      .headers { it.setAll(overrideHeadersFrom(queryParams)) }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html).
   */
  @DeleteMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      NOT_LIFECYCLE,
      NOT_TAGGING
    ]
  )
  @S3Verified(year = 2025)
  fun deleteObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = X_AMZ_IF_MATCH_LAST_MODIFIED_TIME, required = false) matchLastModifiedTime: List<Instant>?,
    @RequestHeader(value = X_AMZ_IF_MATCH_SIZE, required = false) matchSize: List<Long>?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = runCatching {
      // ignore NO_SUCH_KEY exception
      objectService.verifyObjectExists(bucketName, key.key, versionId)
    }.getOrNull()

    objectService.verifyObjectMatching(match, matchLastModifiedTime, matchSize, s3ObjectMetadata)

    val s3ObjectMetadataVersionId = s3ObjectMetadata?.versionId
    val deleted = objectService.deleteObject(bucketName, key.key, versionId)

    return ResponseEntity.noContent()
      .header(X_AMZ_DELETE_MARKER, deleted.toString())
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadataVersionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadataVersionId)
        }
      }
      .headers {
        if (bucket.isVersioningEnabled) {
          try {
            objectService.verifyObjectExists(bucketName, key.key, versionId)
          } catch (e: S3Exception) {
            // ignore all other exceptions here
            if (e === S3Exception.NO_SUCH_KEY_DELETE_MARKER) {
              it.set(X_AMZ_DELETE_MARKER, "true")
            }
          }
        }
      }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      NOT_UPLOADS,
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL,
      NOT_ATTRIBUTES
    ]
  )
  @S3Verified(year = 2025)
  fun getObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_CHECKSUM_MODE, required = false, defaultValue = "DISABLED") mode: ChecksumMode,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = IF_MODIFIED_SINCE, required = false) ifModifiedSince: List<Instant>?,
    @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) ifUnmodifiedSince: List<Instant>?,
    @RequestParam(value = PART_NUMBER, required = false) partNumber: String?,
    @RequestHeader(value = RANGE, required = false) range: HttpRange?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestParam queryParams: Map<String, String>
  ): ResponseEntity<StreamingResponseBody> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match, noneMatch,
      ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata
    )

    range?.let { return getObjectWithRange(it, s3ObjectMetadata) }

    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(s3ObjectMetadata.size.toLong())
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .headers { s3ObjectMetadata.storeHeaders?.let(it::setAll) }
      .headers { it.setAll(userMetadataHeadersFrom(s3ObjectMetadata)) }
      .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
      .headers {
        if (mode == ChecksumMode.ENABLED) {
          it.setAll(checksumHeaderFrom(s3ObjectMetadata))
        }
      }
      .headers { it.setAll(storageClassHeadersFrom(s3ObjectMetadata)) }
      .headers { it.setAll(overrideHeadersFrom(queryParams)) }
      .body(StreamingResponseBody { Files.copy(s3ObjectMetadata.dataPath, it) })
  }

  /**
   * Adds an ACL to an object.
   * This method accepts a String instead of the POJO. We need to use JAX-B annotations
   * instead of Jackson annotations because AWS decided to use xsi:type annotations in the XML
   * representation, which are not supported by Jackson.
   * It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
   * :-(
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      ACL
    ]
  )
  @S3Verified(year = 2025)
  fun putObjectAcl(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_ACL, required = false) cannedAcl: ObjectCannedACL?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody(required = false) body: AccessControlPolicy?
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    val policy: AccessControlPolicy?
    policy = body
      ?: if (cannedAcl != null) {
        policyForCannedAcl(cannedAcl)
      } else {
        return ResponseEntity.badRequest().build()
      }
    objectService.setAcl(bucketName, key.key, versionId, policy)
    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build()
  }

  /**
   * Gets ACL of an object.
   * This method returns a String instead of the POJO. We need to use JAX-B annotations
   * instead of Jackson annotations because AWS decided to use xsi:type annotations in the XML
   * representation, which are not supported by Jackson.
   * It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
   * :-(
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html)
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      ACL
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun getObjectAcl(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<AccessControlPolicy> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    val acl = objectService.getAcl(bucketName, key.key, versionId)
    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .body(acl)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      TAGGING
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_XML_VALUE + ";charset=UTF-8"
    ]
  )
  @S3Verified(year = 2025)
  fun getObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<Tagging> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)

    val tagging = s3ObjectMetadata.tags
      ?.takeIf { it.isNotEmpty() }
      ?.let { Tagging(TagSet(it)) }

    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .lastModified(s3ObjectMetadata.lastModified)
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .body(tagging)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      TAGGING
    ]
  )
  @S3Verified(year = 2025)
  fun putObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody body: Tagging
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectTags(body.tagSet.tags)
    objectService.setObjectTags(bucketName, key.key, versionId, body.tagSet.tags)
    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .lastModified(s3ObjectMetadata.lastModified)
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html).
   */
  @DeleteMapping(
    value = ["/{bucketName:.+}/{*key}"], params = [TAGGING
    ]
  )
  @S3Verified(year = 2025)
  fun deleteObjectTagging(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.setObjectTags(bucketName, key.key, versionId, null)
    return ResponseEntity
      .noContent()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html).
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      LEGAL_HOLD
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun getLegalHold(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<LegalHold> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectLockConfiguration(
      bucketName, key.key,
      versionId
    )

    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .body(s3ObjectMetadata.legalHold)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      LEGAL_HOLD
    ]
  )
  @S3Verified(year = 2025)
  fun putLegalHold(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody body: LegalHold
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.setLegalHold(bucketName, key.key, versionId, body)
    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html).
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      RETENTION
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  fun getObjectRetention(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<Retention> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectLockConfiguration(bucketName, key.key, versionId)

    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .body(s3ObjectMetadata.retention)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      RETENTION
    ]
  )
  @S3Verified(year = 2025)
  fun putObjectRetention(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
    @RequestBody body: Retention
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketObjectLockEnabled(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyRetention(body)
    objectService.setRetention(bucketName, key.key, versionId, body)
    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .build<Void>()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      ATTRIBUTES
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
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
    @RequestParam(value = VERSION_ID, required = false) versionId: String?
  ): ResponseEntity<GetObjectAttributesOutput> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    // this is for either an object request, or a parts request.
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match, noneMatch,
      ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata
    )
    // S3Mock stores the etag with the additional quotation marks needed in the headers. This
    // response does not use eTag as a header, so it must not contain the quotation marks.
    val etag = normalizeEtag(s3ObjectMetadata.etag)!!.replace("\"", "")
    val objectSize = s3ObjectMetadata.size.toLong()
    // in object attributes, S3 returns STANDARD, in all other APIs it returns null...
    val storageClass = s3ObjectMetadata.storageClass ?: StorageClass.STANDARD
    val response = GetObjectAttributesOutput(
      Checksum.from(s3ObjectMetadata),
      if (objectAttributes.contains(ObjectAttributes.ETAG.toString()))
        etag
      else
        null,
      null,  // parts not supported right now
      if (objectAttributes.contains(ObjectAttributes.OBJECT_SIZE.toString()))
        objectSize
      else
        null,
      if (objectAttributes.contains(ObjectAttributes.STORAGE_CLASS.toString()))
        storageClass
      else
        null
    )

    return ResponseEntity
      .ok()
      .lastModified(s3ObjectMetadata.lastModified)
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .body(response)
  }


  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL
    ],
    headers = [
      NOT_X_AMZ_COPY_SOURCE
    ]
  )
  @S3Verified(year = 2025)
  fun putObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_TAGGING, required = false) tags: List<Tag>?,
    @RequestHeader(value = CONTENT_TYPE, required = false) contentType: String?,
    @RequestHeader(value = CONTENT_MD5, required = false) contentMd5: String?,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false, defaultValue = "STANDARD") storageClass: StorageClass,
    @RequestHeader httpHeaders: HttpHeaders,
    inputStream: InputStream
  ): ResponseEntity<Void> {
    var checksum: String? = null
    var checksumAlgorithm: ChecksumAlgorithm? = null

    val (tempFile, calculatedChecksum) = objectService.toTempFile(inputStream, httpHeaders)

    checksumAlgorithmFromSdk(httpHeaders)?.let {
      checksum = calculatedChecksum
      checksumAlgorithm = it
    }
    checksumAlgorithmFromHeader(httpHeaders)?.let {
      checksum = checksumFrom(httpHeaders)
      checksumAlgorithm = it
    }

    val bucket = bucketService.verifyBucketExists(bucketName)
    objectService.verifyObjectMatching(bucketName, key.key, match, noneMatch)
    objectService.verifyMd5(tempFile, contentMd5)
    if (checksum != null) {
      objectService.verifyChecksum(tempFile, checksum!!, checksumAlgorithm!!)
    }

    val s3ObjectMetadata = objectService.putS3Object(
      bucketName = bucketName,
      key = key.key,
      contentType = mediaTypeFrom(contentType).toString(),
      storeHeaders = storeHeadersFrom(httpHeaders),
      path = tempFile,
      userMetadata = userMetadataFrom(httpHeaders),
      encryptionHeaders = encryptionHeadersFrom(httpHeaders),
      tags = tags,
      checksumAlgorithm = checksumAlgorithm,
      checksum = checksum,
      owner = Owner.DEFAULT_OWNER,
      storageClass = storageClass
    )


    runCatching { tempFile.toFile().deleteRecursively() }

    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .headers { it.setAll(checksumHeaderFrom(s3ObjectMetadata)) }
      .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
      .header(X_AMZ_OBJECT_SIZE, s3ObjectMetadata.size)
      .lastModified(s3ObjectMetadata.lastModified)
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    headers = [
      X_AMZ_COPY_SOURCE
    ],
    params = [
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun copyObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_COPY_SOURCE) copySource: CopySource,
    @RequestHeader(value = X_AMZ_METADATA_DIRECTIVE, defaultValue = "COPY") metadataDirective: MetadataDirective,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, required = false) ifModifiedSince: List<Instant>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, required = false) ifUnmodifiedSince: List<Instant>?,
    @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false) storageClass: StorageClass?,
    @RequestHeader httpHeaders: HttpHeaders
  ): ResponseEntity<CopyObjectResult> {
    val targetBucket = bucketService.verifyBucketExists(bucketName)
    val sourceBucket = bucketService.verifyBucketExists(copySource.bucket)
    val s3ObjectMetadata = objectService.verifyObjectExists(copySource.bucket, copySource.key, copySource.versionId)
    objectService.verifyObjectMatchingForCopy(
      match, noneMatch,
      ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata
    )

    val (userMetadata, storeHeaders) =
      if (MetadataDirective.REPLACE == metadataDirective) {
        userMetadataFrom(httpHeaders) to storeHeadersFrom(httpHeaders)
      } else {
        emptyMap<String, String>() to emptyMap()
      }

    val copyS3ObjectMetadata = objectService.copyS3Object(
      copySource.bucket,
      copySource.key,
      copySource.versionId,
      bucketName,
      key.key,
      encryptionHeadersFrom(httpHeaders),
      storeHeaders,
      userMetadata,
      storageClass
    )

    if (copyS3ObjectMetadata == null) {
      return ResponseEntity
        .notFound()
        .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
        .build()
    }

    return ResponseEntity
      .ok()
      .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
      .headers {
        if (sourceBucket.isVersioningEnabled && copySource.versionId != null) {
          it.set(X_AMZ_COPY_SOURCE_VERSION_ID, copySource.versionId)
        }
      }
      .headers {
        if (targetBucket.isVersioningEnabled && copyS3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, copyS3ObjectMetadata.versionId)
        }
      }
      .body(CopyObjectResult(copyS3ObjectMetadata))
  }

  private fun getObjectWithRange(
    range: HttpRange,
    s3ObjectMetadata: S3ObjectMetadata
  ): ResponseEntity<StreamingResponseBody> {
    val fileSize = s3ObjectMetadata.dataPath.toFile().length()
    val startInclusive = range.getRangeStart(fileSize)
    val endInclusive = min(fileSize - 1, range.getRangeEnd(fileSize))
    val contentLength = endInclusive - startInclusive + 1

    if (contentLength < 0 || fileSize <= startInclusive) {
      throw S3Exception.INVALID_RANGE;
    }

    return ResponseEntity
      .status(PARTIAL_CONTENT)
      .headers { applyS3MetadataHeaders(it, s3ObjectMetadata) }
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .header(CONTENT_RANGE, String.format("bytes %d-%d/%d", startInclusive, endInclusive, fileSize))
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(contentLength)
      .body(StreamingResponseBody {
        extractBytesToOutputStream(
          startInclusive,
          s3ObjectMetadata,
          it,
          contentLength
        )
      }
      )
  }

  private fun applyS3MetadataHeaders(headers: HttpHeaders, metadata: S3ObjectMetadata) {
    headers.setAll(userMetadataHeadersFrom(metadata))
    metadata.storeHeaders?.let(headers::setAll)
    metadata.encryptionHeaders?.let(headers::setAll)
  }

  companion object {
    private const val RANGES_BYTES = "bytes"

    private fun extractBytesToOutputStream(
      startOffset: Long,
      s3ObjectMetadata: S3ObjectMetadata,
      outputStream: OutputStream,
      bytesToRead: Long
    ) {
      Files.newInputStream(s3ObjectMetadata.dataPath).use { fis ->
        val skipped = fis.skip(startOffset)
        require(skipped == startOffset) { "Could not skip exact byte range" }
        BoundedInputStream(fis, bytesToRead).use { bis ->
          bis.transferTo(outputStream)
        }
      }
    }
  }
}
