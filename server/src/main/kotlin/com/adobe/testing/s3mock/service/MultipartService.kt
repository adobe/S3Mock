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
package com.adobe.testing.s3mock.service

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.CopyPartResult
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult
import com.adobe.testing.s3mock.dto.ListPartsResult
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.Prefix
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.MultipartStore
import com.adobe.testing.s3mock.store.MultipartUploadInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpRange
import software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes
import java.nio.file.Path
import java.util.Date
import java.util.UUID

open class MultipartService(private val bucketStore: BucketStore, private val multipartStore: MultipartStore) :
  ServiceBase() {
  fun putPart(
    bucketName: String,
    key: String,
    uploadId: UUID,
    partNumber: Int,
    path: Path,
    encryptionHeaders: Map<String, String>
  ): String? {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: return null
    return multipartStore.putPart(
      bucketMetadata,
      uuid,
      uploadId,
      partNumber,
      path,
      encryptionHeaders
    )
  }

  fun copyPart(
    bucketName: String,
    key: String,
    copyRange: HttpRange?,
    partNumber: Int,
    destinationBucket: String,
    destinationKey: String,
    uploadId: UUID,
    encryptionHeaders: Map<String, String>,
    versionId: String?
  ): CopyPartResult? {
    val sourceBucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucket)
    val sourceId = sourceBucketMetadata.getID(key) ?: return null
    // source must be copied to destination
    val destinationId = bucketStore.addKeyToBucket(destinationKey, destinationBucket)
    try {
      val partEtag =
        multipartStore.copyPart(
          sourceBucketMetadata,
          sourceId,
          copyRange,
          partNumber,
          destinationBucketMetadata,
          destinationId,
          uploadId,
          encryptionHeaders,
          versionId
        )
      return CopyPartResult.from(Date(), "\"$partEtag\"")
    } catch (e: Exception) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket)
      throw IllegalStateException(
          "Could not copy part. sourceBucket=$sourceBucketMetadata, destinationBucket=$destinationBucketMetadata, "
            + "key=$key, sourceId=$sourceId, destinationId=$destinationId, uploadId=$uploadId"
        , e
      )
    }
  }

  fun getMultipartUploadParts(
    bucketName: String,
    key: String,
    maxParts: Int,
    partNumberMarker: Int?,
    uploadId: UUID
  ): ListPartsResult? {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key) ?: return null
    val multipartUpload = multipartStore.getMultipartUpload(bucketMetadata, uploadId, false)
    var parts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)

    parts = filterBy(parts, Part::partNumber, partNumberMarker)

    var nextPartNumberMarker: Int? = null
    var isTruncated = false
    if (parts.size > maxParts) {
      parts = parts.subList(0, maxParts)
      nextPartNumberMarker = parts[maxParts - 1].partNumber
      isTruncated = true
    }

    return ListPartsResult(
      bucketName,
      multipartUpload.checksumAlgorithm,
      multipartUpload.checksumType,
      multipartUpload.initiator,
      isTruncated,
      key,
      maxParts,
      nextPartNumberMarker,
      multipartUpload.owner,
      parts,
      partNumberMarker,
      multipartUpload.storageClass,
      uploadId.toString(),
    )
  }

  fun abortMultipartUpload(bucketName: String, key: String, uploadId: UUID) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key)
    try {
      multipartStore.abortMultipartUpload(bucketMetadata, id!!, uploadId)
    } finally {
      bucketStore.removeFromBucket(key, bucketName)
    }
  }

  fun completeMultipartUpload(
    bucketName: String,
    key: String,
    uploadId: UUID,
    parts: List<CompletedPart>,
    encryptionHeaders: Map<String, String>,
    location: String,
    checksum: String?,
    checksumType: ChecksumType?,
    checksumAlgorithm: ChecksumAlgorithm?
  ): CompleteMultipartUploadResult? {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key) ?: return null
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucketMetadata, uploadId)
    return multipartStore
      .completeMultipartUpload(
        bucketMetadata,
        key,
        id,
        uploadId,
        parts,
        encryptionHeaders,
        multipartUploadInfo,
        location,
        checksum,
        checksumType,
        checksumAlgorithm
      )
  }

  fun createMultipartUpload(
    bucketName: String,
    key: String,
    contentType: String?,
    storeHeaders: Map<String, String>,
    owner: Owner,
    initiator: Owner,
    userMetadata: Map<String, String>,
    encryptionHeaders: Map<String, String>,
    tags: List<Tag>?,
    storageClass: StorageClass,
    checksumType: ChecksumType?,
    checksumAlgorithm: ChecksumAlgorithm?
  ): InitiateMultipartUploadResult {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketStore.addKeyToBucket(key, bucketName)

    try {
      val multipartUpload = multipartStore.createMultipartUpload(
        bucketMetadata,
        key,
        id,
        contentType,
        storeHeaders,
        owner,
        initiator,
        userMetadata,
        encryptionHeaders,
        tags,
        storageClass,
        checksumType,
        checksumAlgorithm
      )
      return InitiateMultipartUploadResult(bucketName, key, multipartUpload.uploadId)
    } catch (e: Exception) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(key, bucketName)
      throw IllegalStateException("Could not prepare Multipart Upload. bucket=$bucketMetadata, key=$key, id=$id", e)
    }
  }

  fun listMultipartUploads(
    bucketName: String,
    delimiter: String?,
    encodingType: String?,
    keyMarker: String?,
    maxUploads: Int,
    prefix: String?,
    uploadIdMarker: String?
  ): ListMultipartUploadsResult {
    var nextKeyMarker: String? = null
    var nextUploadIdMarker: String? = null
    var isTruncated = false
    val normalizedPrefix = prefix ?: ""

    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    var contents = multipartStore
      .listMultipartUploads(bucketMetadata, prefix)
      .filter { it.key.startsWith(normalizedPrefix) }
      .sortedBy(MultipartUpload::key)
      .toList()

    contents = filterBy(contents, MultipartUpload::key, keyMarker)

    val commonPrefixes = collapseCommonPrefixes(
      prefix,
      delimiter,
      contents,
      MultipartUpload::key
    )
    contents = filterBy(contents, MultipartUpload::key, commonPrefixes)
    if (maxUploads < contents.size) {
      contents = contents.subList(0, maxUploads)
      isTruncated = true
      if (maxUploads > 0) {
        nextKeyMarker = contents[maxUploads - 1].key
        nextUploadIdMarker = contents[maxUploads - 1].uploadId
      }
    }

    var returnDelimiter = delimiter
    var returnKeyMarker = keyMarker
    var returnPrefix = prefix
    var returnCommonPrefixes = commonPrefixes

    if ("url" == encodingType) {
      contents = contents.map {
        MultipartUpload(
          it.checksumAlgorithm,
          it.checksumType,
          it.initiated,
          it.initiator,
          urlEncodeIgnoreSlashes(it.key),
          it.owner,
          it.storageClass,
          it.uploadId
        )
      }
      returnPrefix = urlEncodeIgnoreSlashes(prefix)
      returnCommonPrefixes = commonPrefixes.map { urlEncodeIgnoreSlashes(it) }
      returnDelimiter = urlEncodeIgnoreSlashes(delimiter)
      returnKeyMarker = urlEncodeIgnoreSlashes(keyMarker)
      nextKeyMarker = urlEncodeIgnoreSlashes(nextKeyMarker)
    }

    return ListMultipartUploadsResult(
      bucketName,
      returnCommonPrefixes.map { Prefix(it) },
      returnDelimiter,
      encodingType,
      isTruncated,
      returnKeyMarker,
      maxUploads,
      nextKeyMarker,
      nextUploadIdMarker,
      returnPrefix,
      contents,
      uploadIdMarker
    )
  }

  fun verifyPartNumberLimits(partNumber: String): Int {
    val number = partNumber.toInt()
    if (number !in 1..10000) {
      LOG.error("Multipart part number invalid. partNumber={}", partNumber)
      throw S3Exception.INVALID_PART_NUMBER
    }
    return number
  }

  @Throws(S3Exception::class)
  fun verifyMultipartParts(
    bucketName: String,
    key: String,
    uploadId: UUID,
    requestedParts: List<CompletedPart>
  ) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key) ?: throw S3Exception.INVALID_PART
    verifyMultipartParts(bucketName, id, uploadId)

    val uploadedParts: List<Part> = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)
    val uploadedPartsMap: Map<Int, String?> = uploadedParts.associate { it.partNumber to it.etag }

    var prevPartNumber = 0
    for (part in requestedParts) {
      if (!uploadedPartsMap.containsKey(part.partNumber) || uploadedPartsMap[part.partNumber] != part.etag) {
        LOG.error(
          "Multipart part not valid. bucket={}, id={}, uploadId={}, partNumber={}",
          bucketMetadata, id, uploadId, part.partNumber
        )
        throw S3Exception.INVALID_PART
      }
      if (part.partNumber < prevPartNumber) {
        LOG.error(
          "Multipart parts order invalid. bucket={}, id={}, uploadId={}, partNumber={}",
          bucketMetadata, id, uploadId, part.partNumber
        )
        throw S3Exception.INVALID_PART_ORDER
      }
      prevPartNumber = part.partNumber
    }
  }

  @Throws(S3Exception::class)
  fun verifyMultipartParts(
    bucketName: String,
    id: UUID,
    uploadId: UUID
  ) {
    verifyMultipartUploadExists(bucketName, uploadId)
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uploadedParts: List<Part> = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)
    for (i in 0..<uploadedParts.size - 1) {
      val part = uploadedParts[i]
      verifyPartNumberLimits(part.partNumber.toString())
      if (part.size < MINIMUM_PART_SIZE) {
        LOG.error(
          "Multipart part size too small. bucket={}, id={}, uploadId={}, size={}",
          bucketMetadata, id, uploadId, part.size
        )
        throw S3Exception.ENTITY_TOO_SMALL
      }
    }
  }

  @Throws(S3Exception::class)
  fun verifyMultipartUploadExists(bucketName: String, uploadId: UUID) {
    verifyMultipartUploadExists(bucketName, uploadId, false)
  }

  @Throws(S3Exception::class)
  fun verifyMultipartUploadExists(
    bucketName: String,
    uploadId: UUID,
    includeCompleted: Boolean
  ): MultipartUploadInfo? {
    try {
      val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
      multipartStore.getMultipartUpload(bucketMetadata, uploadId, includeCompleted)
      return multipartStore.getMultipartUploadInfo(bucketMetadata, uploadId)
    } catch (_: IllegalArgumentException) {
      throw S3Exception.NO_SUCH_UPLOAD_MULTIPART
    }
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(MultipartService::class.java)
    const val MINIMUM_PART_SIZE: Long = 5L * 1024L * 1024L
  }
}
