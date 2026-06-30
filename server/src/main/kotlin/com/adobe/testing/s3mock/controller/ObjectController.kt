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
import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumMode
import com.adobe.testing.s3mock.dto.CopyObjectResult
import com.adobe.testing.s3mock.dto.CopySource
import com.adobe.testing.s3mock.dto.Delete
import com.adobe.testing.s3mock.dto.DeleteResult
import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.model.S3ObjectMetadata
import com.adobe.testing.s3mock.model.toCopyObjectResult
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective
import com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_MODE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_LAST_MODIFIED_TIME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_SIZE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_SIZE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING
import com.adobe.testing.s3mock.util.AwsHttpParameters.DELETE
import com.adobe.testing.s3mock.util.AwsHttpParameters.FILE
import com.adobe.testing.s3mock.util.AwsHttpParameters.KEY
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
import com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID
import com.adobe.testing.s3mock.util.BoundedInputStream
import com.adobe.testing.s3mock.util.HeaderUtil.encryptionHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.mediaTypeFrom
import com.adobe.testing.s3mock.util.HeaderUtil.storeHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.userMetadataFrom
import com.adobe.testing.s3mock.util.checksumHeader
import com.adobe.testing.s3mock.util.objectMetadataHeaders
import com.adobe.testing.s3mock.util.resolveChecksum
import com.adobe.testing.s3mock.util.storageClassHeaders
import com.adobe.testing.s3mock.util.versionHeader
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpHeaders.ACCEPT_RANGES
import org.springframework.http.HttpHeaders.CONTENT_RANGE
import org.springframework.http.HttpHeaders.CONTENT_TYPE
import org.springframework.http.HttpHeaders.IF_MATCH
import org.springframework.http.HttpHeaders.IF_MODIFIED_SINCE
import org.springframework.http.HttpHeaders.IF_NONE_MATCH
import org.springframework.http.HttpHeaders.IF_UNMODIFIED_SINCE
import org.springframework.http.HttpRange
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
import java.time.Instant
import kotlin.io.path.inputStream
import kotlin.math.min

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class ObjectController(
  private val bucketService: BucketService,
  private val objectService: ObjectService,
) {
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
      "/{bucketName:.+}/",
    ],
    params = [
      DELETE,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
  )
  @S3Verified(year = 2025)
  fun deleteObjects(
    @PathVariable bucketName: String,
    @RequestBody body: Delete,
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
      "/{bucketName:.+}/",
    ],
    params = [
      NOT_DELETE,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
    consumes = [
      MediaType.MULTIPART_FORM_DATA_VALUE,
    ],
  )
  @Throws(IOException::class)
  fun postObject(
    @PathVariable bucketName: String,
    @RequestParam(value = KEY) key: ObjectKey,
    @RequestParam(value = TAGGING, required = false) tags: List<Tag>?,
    @RequestParam(value = CONTENT_TYPE, required = false) contentType: String?,
    @RequestParam(value = CONTENT_MD5, required = false) contentMd5: String?,
    @RequestParam(value = X_AMZ_STORAGE_CLASS, required = false, defaultValue = "STANDARD") storageClass: StorageClass,
    @RequestPart(FILE) file: MultipartFile,
  ): ResponseEntity<Void> {
    val (tempFile, _) = objectService.toTempFile(file.inputStream)

    val bucket = bucketService.verifyBucketExists(bucketName)
    objectService.verifyMd5(tempFile, contentMd5)

    val s3ObjectMetadata =
      objectService.putObject(
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
        storageClass = storageClass,
      )

    runCatching { tempFile.toFile().deleteRecursively() }

    return ResponseEntity
      .ok()
      .headers {
        s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll)
        s3ObjectMetadata.checksumHeader().let(it::setAll)
        s3ObjectMetadata.encryptionHeaders?.let(it::setAll)
      }.lastModified(s3ObjectMetadata.lastModified)
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
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
      "/{bucketName:.+}/{*key}",
    ],
    method = [
      RequestMethod.HEAD,
    ],
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
    @RequestParam queryParams: Map<String, String>,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match,
      noneMatch,
      ifModifiedSince,
      ifUnmodifiedSince,
      s3ObjectMetadata,
    )

    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(s3ObjectMetadata.size.toLong())
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .headers {
        s3ObjectMetadata.objectMetadataHeaders(bucket.isVersioningEnabled, queryParams).let(it::setAll)
      }.build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html).
   */
  @DeleteMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      NOT_LIFECYCLE,
      NOT_TAGGING,
    ],
  )
  @S3Verified(year = 2025)
  fun deleteObject(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = X_AMZ_IF_MATCH_LAST_MODIFIED_TIME, required = false) matchLastModifiedTime: List<Instant>?,
    @RequestHeader(value = X_AMZ_IF_MATCH_SIZE, required = false) matchSize: List<Long>?,
    @RequestParam(value = VERSION_ID, required = false) versionId: String?,
  ): ResponseEntity<Void> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val s3ObjectMetadata =
      runCatching {
        // ignore NO_SUCH_KEY exception — object may not exist, delete still proceeds
        objectService.verifyObjectExists(bucketName, key.key, versionId)
      }.getOrNull()

    objectService.verifyObjectMatching(match, matchLastModifiedTime, matchSize, s3ObjectMetadata)

    val outcome = objectService.deleteObject(bucketName, key.key, versionId)

    return ResponseEntity
      .noContent()
      .header(X_AMZ_DELETE_MARKER, outcome.isDeleteMarker.toString())
      .headers {
        s3ObjectMetadata?.versionHeader(bucket.isVersioningEnabled)?.let(it::setAll)
      }.build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      NOT_UPLOADS,
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL,
      NOT_ATTRIBUTES,
    ],
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
    @RequestParam queryParams: Map<String, String>,
  ): ResponseEntity<StreamingResponseBody> {
    val bucket = bucketService.verifyBucketExists(bucketName)

    val s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key, versionId)
    objectService.verifyObjectMatching(
      match,
      noneMatch,
      ifModifiedSince,
      ifUnmodifiedSince,
      s3ObjectMetadata,
    )

    range?.let {
      return getObjectWithRange(
        it,
        s3ObjectMetadata,
        bucket.isVersioningEnabled,
        mode,
        queryParams,
      )
    }

    return ResponseEntity
      .ok()
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(s3ObjectMetadata.size.toLong())
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .headers {
        s3ObjectMetadata.objectMetadataHeaders(bucket.isVersioningEnabled, queryParams, mode == ChecksumMode.ENABLED).let(it::setAll)
      }.body(StreamingResponseBody { s3ObjectMetadata.dataPath.inputStream().transferTo(it) })
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    params = [
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL,
    ],
    headers = [
      NOT_X_AMZ_COPY_SOURCE,
    ],
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
    inputStream: InputStream,
  ): ResponseEntity<Void> {
    val (tempFile, calculatedChecksum) = objectService.toTempFile(inputStream, httpHeaders)
    val (checksum, checksumAlgorithm) = resolveChecksum(httpHeaders, calculatedChecksum)

    val bucket = bucketService.verifyBucketExists(bucketName)
    objectService.verifyObjectMatching(bucketName, key.key, match, noneMatch)
    objectService.verifyMd5(tempFile, contentMd5)
    checksum?.let {
      objectService.verifyChecksum(tempFile, it, checksumAlgorithm!!)
    }

    val s3ObjectMetadata =
      objectService.putObject(
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
        storageClass = storageClass,
      )

    runCatching { tempFile.toFile().deleteRecursively() }

    return ResponseEntity
      .ok()
      .headers {
        s3ObjectMetadata.versionHeader(bucket.isVersioningEnabled).let(it::setAll)
        s3ObjectMetadata.checksumHeader().let(it::setAll)
        s3ObjectMetadata.encryptionHeaders?.let(it::setAll)
      }.header(X_AMZ_OBJECT_SIZE, s3ObjectMetadata.size)
      .lastModified(s3ObjectMetadata.lastModified)
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}",
    ],
    headers = [
      X_AMZ_COPY_SOURCE,
    ],
    params = [
      NOT_UPLOAD_ID,
      NOT_TAGGING,
      NOT_LEGAL_HOLD,
      NOT_RETENTION,
      NOT_ACL,
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE,
    ],
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
    @RequestHeader httpHeaders: HttpHeaders,
  ): ResponseEntity<CopyObjectResult> {
    val targetBucket = bucketService.verifyBucketExists(bucketName)
    val sourceBucket = bucketService.verifyBucketExists(copySource.bucket)
    val s3ObjectMetadata = objectService.verifyObjectExists(copySource.bucket, copySource.key, copySource.versionId)
    objectService.verifyObjectMatchingForCopy(
      match,
      noneMatch,
      ifModifiedSince,
      ifUnmodifiedSince,
      s3ObjectMetadata,
    )

    val (userMetadata, storeHeaders) =
      if (MetadataDirective.REPLACE == metadataDirective) {
        userMetadataFrom(httpHeaders) to storeHeadersFrom(httpHeaders)
      } else {
        emptyMap<String, String>() to emptyMap()
      }

    val copyS3ObjectMetadata =
      objectService.copyObject(
        copySource.bucket,
        copySource.key,
        copySource.versionId,
        bucketName,
        key.key,
        encryptionHeadersFrom(httpHeaders),
        storeHeaders,
        userMetadata,
        storageClass,
      )

    if (copyS3ObjectMetadata == null) {
      return ResponseEntity
        .notFound()
        .headers { s3ObjectMetadata.encryptionHeaders?.let(it::setAll) }
        .build()
    }

    return ResponseEntity
      .ok()
      .headers {
        s3ObjectMetadata.encryptionHeaders?.let(it::setAll)
        copySource.versionHeader(sourceBucket.isVersioningEnabled).let(it::setAll)
        copyS3ObjectMetadata.versionHeader(targetBucket.isVersioningEnabled).let(it::setAll)
      }.body(copyS3ObjectMetadata.toCopyObjectResult())
  }

  private fun getObjectWithRange(
    range: HttpRange,
    s3ObjectMetadata: S3ObjectMetadata,
    versioning: Boolean,
    mode: ChecksumMode,
    queryParams: Map<String, String>,
  ): ResponseEntity<StreamingResponseBody> {
    val fileSize = s3ObjectMetadata.dataPath.toFile().length()
    val startInclusive = range.getRangeStart(fileSize)
    val endInclusive = min(fileSize - 1, range.getRangeEnd(fileSize))
    val contentLength = endInclusive - startInclusive + 1

    if (contentLength < 0 || fileSize <= startInclusive) {
      throw S3Exception.INVALID_RANGE
    }

    return ResponseEntity
      .status(PARTIAL_CONTENT)
      .eTag(normalizeEtag(s3ObjectMetadata.etag))
      .lastModified(s3ObjectMetadata.lastModified)
      .contentLength(contentLength)
      .contentType(mediaTypeFrom(s3ObjectMetadata.contentType))
      .header(ACCEPT_RANGES, RANGES_BYTES)
      .header(CONTENT_RANGE, "bytes $startInclusive-$endInclusive/$fileSize")
      .headers {
        s3ObjectMetadata.objectMetadataHeaders(versioning, queryParams, mode == ChecksumMode.ENABLED).let(it::setAll)
      }.body(
        StreamingResponseBody {
          extractBytesToOutputStream(
            startInclusive,
            s3ObjectMetadata,
            it,
            contentLength,
          )
        },
      )
  }

  companion object {
    private const val RANGES_BYTES = "bytes"

    private fun extractBytesToOutputStream(
      startOffset: Long,
      s3ObjectMetadata: S3ObjectMetadata,
      outputStream: OutputStream,
      bytesToRead: Long,
    ) {
      s3ObjectMetadata.dataPath.inputStream().use { fis ->
        val skipped = fis.skip(startOffset)
        require(skipped == startOffset) { "Could not skip exact byte range" }
        BoundedInputStream(fis, bytesToRead).use { bis ->
          bis.transferTo(outputStream)
        }
      }
    }
  }
}
