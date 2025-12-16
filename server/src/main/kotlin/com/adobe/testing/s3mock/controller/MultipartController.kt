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
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.dto.CopyPartResult
import com.adobe.testing.s3mock.dto.CopySource
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult
import com.adobe.testing.s3mock.dto.ListPartsResult
import com.adobe.testing.s3mock.dto.ObjectKey
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.service.MultipartService
import com.adobe.testing.s3mock.service.ObjectService
import com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE_RANGE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_TYPE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_RANGE
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_VERSION_ID
import com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE
import com.adobe.testing.s3mock.util.AwsHttpParameters.KEY_MARKER
import com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_PARTS
import com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_UPLOADS
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE
import com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER
import com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER_MARKER
import com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOADS
import com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID
import com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID_MARKER
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromHeader
import com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk
import com.adobe.testing.s3mock.util.HeaderUtil.checksumFrom
import com.adobe.testing.s3mock.util.HeaderUtil.checksumHeaderFrom
import com.adobe.testing.s3mock.util.HeaderUtil.checksumTypeFrom
import com.adobe.testing.s3mock.util.HeaderUtil.encryptionHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.storeHeadersFrom
import com.adobe.testing.s3mock.util.HeaderUtil.userMetadataFrom
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpHeaders.IF_MATCH
import org.springframework.http.HttpHeaders.IF_NONE_MATCH
import org.springframework.http.HttpRange
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
import org.springframework.web.bind.annotation.RequestParam
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.io.IOException
import java.io.InputStream
import java.time.Instant
import java.util.UUID

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class MultipartController(
  private val bucketService: BucketService, private val objectService: ObjectService,
  private val multipartService: MultipartService
) {
  // ===============================================================================================
  // /{bucketName:.+}
  // ===============================================================================================
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      UPLOADS
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun listMultipartUploads(
    @PathVariable bucketName: String,
    @RequestParam(required = false) delimiter: String?,
    @RequestParam(name = ENCODING_TYPE, required = false) encodingType: String?,
    @RequestParam(name = KEY_MARKER, required = false) keyMarker: String?,
    @RequestParam(name = MAX_UPLOADS, defaultValue = "1000", required = false) maxUploads: Int,
    @RequestParam(required = false) prefix: String?,
    @RequestParam(name = UPLOAD_ID_MARKER, required = false) uploadIdMarker: String?
  ): ResponseEntity<ListMultipartUploadsResult> {
    bucketService.verifyBucketExists(bucketName)

    val result = multipartService.listMultipartUploads(
      bucketName,
      delimiter,
      encodingType,
      keyMarker,
      maxUploads,
      prefix,
      uploadIdMarker
    )
    return ResponseEntity.ok(result)
  }

  // ===============================================================================================
  // /{bucketName:.+}/{*key}
  // ===============================================================================================
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html).
   */
  @DeleteMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      UPLOAD_ID,
      NOT_LIFECYCLE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun abortMultipartUpload(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam uploadId: UUID
  ): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    multipartService.verifyMultipartUploadExists(bucketName, uploadId)
    multipartService.abortMultipartUpload(bucketName, key.key, uploadId)
    return ResponseEntity.noContent().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html).
   */
  @GetMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      UPLOAD_ID
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun listParts(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam(name = MAX_PARTS, defaultValue = "1000", required = false) maxParts: Int,
    @RequestParam(name = PART_NUMBER_MARKER, required = false) partNumberMarker: Int?,
    @RequestParam uploadId: UUID
  ): ResponseEntity<ListPartsResult> {
    bucketService.verifyBucketExists(bucketName)
    multipartService.verifyMultipartUploadExists(bucketName, uploadId)

    val result = multipartService.getMultipartUploadParts(
      bucketName,
      key.key,
      maxParts,
      partNumberMarker,
      uploadId
    )
    return ResponseEntity
      .ok(result)
  }


  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      UPLOAD_ID,
      PART_NUMBER
    ],
    headers = [
      NOT_X_AMZ_COPY_SOURCE,
      NOT_X_AMZ_COPY_SOURCE_RANGE
    ]
  )
  @S3Verified(year = 2025)
  fun uploadPart(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestParam uploadId: UUID,
    @RequestParam partNumber: String,
    @RequestHeader httpHeaders: HttpHeaders,
    inputStream: InputStream
  ): ResponseEntity<Void> {
    val (tempFile, sdkChecksum) = multipartService.toTempFile(inputStream, httpHeaders)
    bucketService.verifyBucketExists(bucketName)
    multipartService.verifyMultipartUploadExists(bucketName, uploadId)
    val partNum = multipartService.verifyPartNumberLimits(partNumber)

    val fromSdk = checksumAlgorithmFromSdk(httpHeaders)
    val fromHeader = checksumAlgorithmFromHeader(httpHeaders)
    val (checksum, checksumAlgorithm) = when {
      fromSdk != null    -> sdkChecksum to fromSdk
      fromHeader != null -> checksumFrom(httpHeaders) to fromHeader
      else               -> null to null
    }

    if (checksum != null && checksumAlgorithm != null) {
      multipartService.verifyChecksum(tempFile, checksum, checksumAlgorithm)
    }

    val etag = multipartService.putPart(
      bucketName,
      key.key,
      uploadId,
      partNum,
      tempFile,
      encryptionHeadersFrom(httpHeaders)
    )

    runCatching { tempFile.toFile().deleteRecursively() }

    val checksumHeader = checksumHeaderFrom(checksum, checksumAlgorithm)
    return ResponseEntity
      .ok()
      .headers {
        checksumHeader.let(it::setAll)
        encryptionHeadersFrom(httpHeaders).let(it::setAll)
      }
      .eTag(normalizeEtag(etag))
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html).
   */
  @PutMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    headers = [
      X_AMZ_COPY_SOURCE
    ],
    params = [
      UPLOAD_ID,
      PART_NUMBER
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun uploadPartCopy(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = X_AMZ_COPY_SOURCE) copySource: CopySource,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_RANGE, required = false) copyRange: HttpRange?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, required = false) ifModifiedSince: List<Instant>?,
    @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, required = false) ifUnmodifiedSince: List<Instant>?,
    @RequestParam uploadId: UUID,
    @RequestParam partNumber: String,
    @RequestHeader httpHeaders: HttpHeaders
  ): ResponseEntity<CopyPartResult> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val partNum = multipartService.verifyPartNumberLimits(partNumber)
    val s3ObjectMetadata = objectService.verifyObjectExists(
      copySource.bucket, copySource.key,
      copySource.versionId
    )
    objectService.verifyObjectMatchingForCopy(
      match, noneMatch,
      ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata
    )

    val encryptionHeaders = encryptionHeadersFrom(httpHeaders)
    val result = multipartService.copyPart(
      copySource.bucket,
      copySource.key,
      copyRange,
      partNum,
      bucketName,
      key.key,
      uploadId,
      encryptionHeaders,
      copySource.versionId
    )

    return ResponseEntity
      .ok()
      .headers {
        if (bucket.isVersioningEnabled && s3ObjectMetadata.versionId != null) {
          it.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId)
        }
      }
      .headers { encryptionHeaders.let(it::setAll) }
      .body(result)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html).
   */
  @PostMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      UPLOADS
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun createMultipartUpload(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = HttpHeaders.CONTENT_TYPE, required = false) contentType: String?,
    @RequestHeader(value = X_AMZ_CHECKSUM_TYPE, required = false) checksumType: ChecksumType?,
    @RequestHeader(value = X_AMZ_TAGGING, required = false) tags: List<Tag>?,
    @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false, defaultValue = "STANDARD") storageClass: StorageClass,
    @RequestHeader httpHeaders: HttpHeaders,
    inputStream: InputStream
  ): ResponseEntity<InitiateMultipartUploadResult> {
    bucketService.verifyBucketExists(bucketName)

    try {
      // workaround for AWS CRT-based S3 client: Consume (and discard) body in Initiate Multipart Upload request
      inputStream.use { ins ->
        ins.copyTo(java.io.OutputStream.nullOutputStream())
      }
    } catch (_: IOException) {
      throw S3Exception.BAD_REQUEST_CONTENT
    }

    val encryptionHeaders = encryptionHeadersFrom(httpHeaders)
    val checksumAlgorithm = checksumAlgorithmFromHeader(httpHeaders)
    val result =
      multipartService.createMultipartUpload(
        bucketName,
        key.key,
        contentType,
        storeHeadersFrom(httpHeaders),
        Owner.DEFAULT_OWNER,
        Owner.DEFAULT_OWNER,
        userMetadataFrom(httpHeaders),
        encryptionHeaders,
        tags,
        storageClass,
        checksumType,
        checksumAlgorithm
      )

    return ResponseEntity
      .ok()
      .headers {
        encryptionHeaders.let(it::setAll)
        checksumAlgorithm?.let { alg -> it.set(X_AMZ_CHECKSUM_ALGORITHM, alg.toString()) }
        checksumType?.let { type -> it.set(X_AMZ_CHECKSUM_TYPE, type.toString()) }
      }
      .body(result)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).
   */
  @PostMapping(
    value = [
      "/{bucketName:.+}/{*key}"
    ],
    params = [
      UPLOAD_ID
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun completeMultipartUpload(
    @PathVariable bucketName: String,
    @PathVariable key: ObjectKey,
    @RequestHeader(value = IF_MATCH, required = false) match: List<String>?,
    @RequestHeader(value = IF_NONE_MATCH, required = false) noneMatch: List<String>?,
    @RequestParam uploadId: UUID,
    @RequestBody upload: CompleteMultipartUpload,
    request: HttpServletRequest,
    @RequestHeader httpHeaders: HttpHeaders
  ): ResponseEntity<CompleteMultipartUploadResult> {
    val bucket = bucketService.verifyBucketExists(bucketName)
    val multipartUploadInfo = multipartService.verifyMultipartUploadExists(bucketName, uploadId, true)
    val objectName = key.key
    val isCompleted = multipartUploadInfo?.completed == true
    if (!isCompleted) {
      multipartService.verifyMultipartParts(bucketName, objectName, uploadId, upload.parts)
    }
    val s3ObjectMetadata = objectService.getObject(bucketName, key.key, null)
    objectService.verifyObjectMatching(match, noneMatch, null, null, s3ObjectMetadata)
    val locationWithEncodedKey = request
      .requestURL
      .toString()
      .replace(objectName, SdkHttpUtils.urlEncode(objectName))

    val result: CompleteMultipartUploadResult =
      if (!isCompleted) {
        multipartService.completeMultipartUpload(
          bucketName,
          objectName,
          uploadId,
          upload.parts,
          encryptionHeadersFrom(httpHeaders),
          locationWithEncodedKey,
          checksumFrom(httpHeaders),
          checksumTypeFrom(httpHeaders),
          checksumAlgorithmFromHeader(httpHeaders)
        )!!
      } else {
        CompleteMultipartUploadResult.from(
          locationWithEncodedKey,
          bucketName,
          objectName,
          normalizeEtag(requireNotNull(s3ObjectMetadata).etag),
          multipartUploadInfo,
          s3ObjectMetadata.checksum,
          s3ObjectMetadata.checksumType,
          s3ObjectMetadata.checksumAlgorithm,
          s3ObjectMetadata.versionId
        )
      }

    return ResponseEntity
      .ok()
      .headers {
        result.multipartUploadInfo.encryptionHeaders.let(it::setAll)
        if (bucket.isVersioningEnabled && result.versionId != null) {
          it.set(X_AMZ_VERSION_ID, result.versionId)
        }
      }
      .body(result)
  }
}
