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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.Initiator
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.BoundedInputStream
import com.adobe.testing.s3mock.util.DigestUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpRange
import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.SequenceInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Collections
import java.util.Date
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.outputStream

open class MultipartStore(private val objectStore: ObjectStore, private val objectMapper: ObjectMapper) : StoreBase() {
  /**
   * This map stores one lock object per MultipartUpload ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private val lockStore: MutableMap<UUID, Any> = ConcurrentHashMap()

  fun createMultipartUpload(
    bucket: BucketMetadata,
    key: String,
    id: UUID,
    contentType: String?,
    storeHeaders: Map<String, String>,
    owner: Owner,
    initiator: Initiator,
    userMetadata: Map<String, String>,
    encryptionHeaders: Map<String, String>,
    tags: List<Tag>?,
    storageClass: StorageClass,
    checksumType: ChecksumType?,
    checksumAlgorithm: ChecksumAlgorithm?
  ): MultipartUpload {
    val uploadId = UUID.randomUUID()
    if (!createPartsFolder(bucket, uploadId)) {
      LOG.error(
        "Directories for storing multipart uploads couldn't be created. bucket={}, key={}, "
          + "id={}, uploadId={}", bucket, key, id, uploadId
      )
      throw IllegalStateException(
        "Directories for storing multipart uploads couldn't be created."
      )
    }
    val upload = MultipartUpload(
      checksumAlgorithm,
      ChecksumType.FULL_OBJECT,
      Date(),
      initiator,
      key,
      owner,
      storageClass,
      uploadId.toString()
    )
    val multipartUploadInfo = MultipartUploadInfo(
      upload,
      contentType,
      userMetadata,
      storeHeaders,
      encryptionHeaders,
      bucket.name,
      storageClass,
      tags,
      null,
      checksumType,
      checksumAlgorithm
    )
    lockStore.putIfAbsent(uploadId, Any())
    writeMetafile(bucket, multipartUploadInfo)

    return upload
  }

  fun listMultipartUploads(bucketMetadata: BucketMetadata, prefix: String?): List<MultipartUpload> {
    val multipartsFolder = getMultipartsFolder(bucketMetadata)
    if (!multipartsFolder.toFile().exists()) {
      return emptyList()
    }
    try {
      return multipartsFolder
        .listDirectoryEntries()
        .mapNotNull {
          runCatching {
            getUploadMetadata(bucketMetadata, UUID.fromString(it.fileName.toString()))
          }.getOrNull()
        }
        .filter { !it.completed }
        .map { it.upload }
        .filter { prefix.isNullOrBlank() || it.key.startsWith(prefix) }
        .toList()
    } catch (e: IOException) {
      throw IllegalStateException("Could not load buckets from data directory ", e)
    }
  }

  fun getMultipartUploadInfo(
    bucketMetadata: BucketMetadata,
    uploadId: UUID
  ): MultipartUploadInfo? = getUploadMetadata(bucketMetadata, uploadId)

  fun getMultipartUpload(bucketMetadata: BucketMetadata, uploadId: UUID, includeCompleted: Boolean): MultipartUpload {
    val uploadMetadata = getUploadMetadata(bucketMetadata, uploadId)
    if (uploadMetadata != null) {
      if (includeCompleted) {
        return uploadMetadata.upload
      } else {
        require(!uploadMetadata.completed) { "No active MultipartUpload found with uploadId: $uploadId" }
        return uploadMetadata.upload
      }
    } else {
      throw IllegalArgumentException("No MultipartUpload found with uploadId: $uploadId")
    }
  }

  fun abortMultipartUpload(bucket: BucketMetadata, id: UUID, uploadId: UUID) {
    val multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId)
    if (multipartUploadInfo != null) {
      synchronized(lockStore[uploadId]!!) {
        try {
          getPartsFolder(bucket, uploadId).toFile().deleteRecursively()
        } catch (e: IOException) {
          throw IllegalStateException("Could not delete multipart-directory $uploadId", e)
        }
        lockStore.remove(uploadId)
      }
    }
  }

  fun putPart(
    bucket: BucketMetadata,
    id: UUID,
    uploadId: UUID,
    partNumber: Int,
    path: Path,
    encryptionHeaders: Map<String, String>
  ): String {
    val file = inputPathToFile(path, getPartPath(bucket, uploadId, partNumber))

    return DigestUtil.hexDigest(encryptionHeaders[AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID], file)
  }

  fun completeMultipartUpload(
    bucket: BucketMetadata,
    key: String,
    id: UUID,
    uploadId: UUID,
    parts: List<CompletedPart>,
    encryptionHeaders: Map<String, String>,
    uploadInfo: MultipartUploadInfo?,
    location: String,
    checksum: String?,
    checksumType: ChecksumType?,
    checksumAlgorithm: ChecksumAlgorithm?
  ): CompleteMultipartUploadResult {
    requireNotNull(uploadInfo) { "Unknown upload $uploadId" }
    val partFolder = getPartsFolder(bucket, uploadId)
    val partsPaths: List<Path> = parts.map { part ->
      Paths.get(partFolder.toString(), "${part.partNumber}$PART_SUFFIX")
    }
    val tempFile = Files.createTempFile("completeMultipartUpload", "")
    try {
      toInputStream(partsPaths).use { input ->
        tempFile.outputStream().use { os ->
          input.transferTo(os)
        }
      }
      val checksumFor = validateChecksums(uploadInfo, tempFile,  parts, partsPaths, checksum, checksumType, checksumAlgorithm)
      val etag = DigestUtil.hexDigestMultipart(partsPaths)
      val s3ObjectMetadata = objectStore.storeS3ObjectMetadata(
        bucket,
        id,
        key,
        uploadInfo.contentType,
        uploadInfo.storeHeaders,
        tempFile,
        uploadInfo.userMetadata,
        encryptionHeaders,
        etag,
        uploadInfo.tags,
        uploadInfo.checksumAlgorithm,
        checksumFor,
        uploadInfo.upload.owner,
        uploadInfo.storageClass,
        checksumType
      )
      // delete parts and update MultipartInfo
      partsPaths.forEach { runCatching { it.toFile().deleteRecursively() } }
      val completedUploadInfo = uploadInfo.complete()
      writeMetafile(bucket, completedUploadInfo)
      return CompleteMultipartUploadResult.from(
        location,
        completedUploadInfo.bucket,
        key,
        etag,
        completedUploadInfo,
        checksumFor,
        s3ObjectMetadata.checksumType,
        checksumAlgorithm,
        s3ObjectMetadata.versionId
      )
    } catch (e: IOException) {
      throw IllegalStateException("Error finishing multipart upload bucket=$bucket, key=$key, id=$id, uploadId=$uploadId", e)
    } finally {
      runCatching { Files.deleteIfExists(tempFile) }
    }
  }

  fun getMultipartUploadParts(
    bucket: BucketMetadata,
    id: UUID,
    uploadId: UUID
  ): List<Part> {
    val partsPath = getPartsFolder(bucket, uploadId)
    try {
      return partsPath
        .listDirectoryEntries("*$PART_SUFFIX")
        .map {
            val name = it.fileName.toString()
            val prefix = name.substringBefore('.')
            val partNumber = prefix.toInt()
            val file = it.toFile()
            val partMd5 = DigestUtil.hexDigest(file)
            val lastModified = Date(file.lastModified())
            Part(partNumber, partMd5, lastModified, file.length())
          }
          .sortedBy { it.partNumber }
          .toList()
    } catch (e: IOException) {
      throw IllegalStateException("Could not read all parts. bucket=$bucket, id=$id, uploadId=$uploadId", e)
    }
  }

  fun copyPart(
    bucket: BucketMetadata,
    id: UUID,
    copyRange: HttpRange?,
    partNumber: Int,
    destinationBucket: BucketMetadata,
    destinationId: UUID,
    uploadId: UUID,
    encryptionHeaders: Map<String, String>?,
    versionId: String?
  ): String {
    verifyMultipartUploadPreparation(destinationBucket, destinationId, uploadId)

    return copyPartToFile(
      bucket,
      id,
      copyRange,
      createPartFile(destinationBucket, destinationId, uploadId, partNumber),
      versionId
    )
  }

  private fun copyPartToFile(
    bucket: BucketMetadata,
    id: UUID,
    copyRange: HttpRange?,
    partFile: File,
    versionId: String?
  ): String {
    var from = 0L
    val s3ObjectMetadata = requireNotNull(objectStore.getS3ObjectMetadata(bucket, id, versionId)) {
      "Object metadata not found. bucket=$bucket, id=$id, versionId=$versionId"
    }
    var len = s3ObjectMetadata.dataPath.toFile().length()
    if (copyRange != null) {
      from = copyRange.getRangeStart(len)
      len = copyRange.getRangeEnd(len) - copyRange.getRangeStart(len) + 1
    }

    try {
      s3ObjectMetadata.dataPath.toFile().inputStream().use { sourceStream ->
        partFile.outputStream().use { targetStream ->
          sourceStream.skipNBytes(from)
          BoundedInputStream(sourceStream, len).use { bis ->
            bis.transferTo(targetStream)
          }
        }
      }
    } catch (e: IOException) {
      throw IllegalStateException(
        "Could not copy object. bucket=$bucket, id=$id, range=$copyRange, partFile=$partFile", e
      )
    }
    return DigestUtil.hexDigest(partFile)
  }

  private fun createPartFile(
    bucket: BucketMetadata,
    id: UUID,
    uploadId: UUID,
    partNumber: Int
  ): File {
    val partFile = getPartPath(
      bucket,
      uploadId,
      partNumber
    ).toFile()

    try {
      check(partFile.exists() || partFile.createNewFile()) {
        "Could not create buffer file. bucket=$bucket, id=$id, uploadId=$uploadId, partNumber=$partNumber"
      }
    } catch (e: IOException) {
      throw IllegalStateException(
        "Could not create buffer file. bucket=$bucket, id=$id, uploadId=$uploadId, partNumber=$partNumber", e
      )
    }
    return partFile
  }

  private fun createPartsFolder(bucket: BucketMetadata, uploadId: UUID): Boolean {
    val partsFolder = getPartsFolder(bucket, uploadId).toFile()
    return partsFolder.mkdirs()
  }


  private fun getMultipartsFolder(bucket: BucketMetadata): Path {
    return Paths.get(bucket.path.toString(), MULTIPARTS_FOLDER)
  }

  private fun getPartPath(bucket: BucketMetadata, uploadId: UUID, partNumber: Int): Path {
    return getPartsFolder(bucket, uploadId).resolve(partNumber.toString() + PART_SUFFIX)
  }

  private fun getUploadMetadataPath(bucket: BucketMetadata, uploadId: UUID): Path {
    return getPartsFolder(bucket, uploadId).resolve(MULTIPART_UPLOAD_META_FILE)
  }

  private fun getPartsFolder(bucket: BucketMetadata, uploadId: UUID): Path {
    return getMultipartsFolder(bucket).resolve(uploadId.toString())
  }

  private fun getUploadMetadata(bucket: BucketMetadata, uploadId: UUID): MultipartUploadInfo? {
    val metaPath = getUploadMetadataPath(bucket, uploadId)

    if (metaPath.exists()) {
      synchronized(lockStore[uploadId]!!) {
        try {
          return objectMapper.readValue(
            metaPath.toFile(),
            MultipartUploadInfo::class.java
          )
        } catch (e: IOException) {
          throw IllegalArgumentException("Could not read upload metadata-file $uploadId", e)
        }
      }
    }
    return null
  }

  private fun writeMetafile(bucket: BucketMetadata, uploadInfo: MultipartUploadInfo) {
    val uploadId = uploadInfo.upload.uploadId
    try {
      synchronized(lockStore[UUID.fromString(uploadId)]!!) {
        val metaFile = getUploadMetadataPath(bucket, UUID.fromString(uploadId)).toFile()
        objectMapper.writeValue(metaFile, uploadInfo)
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write upload metadata-file $uploadId", e)
    }
  }

  private fun verifyMultipartUploadPreparation(
    bucket: BucketMetadata,
    id: UUID?,
    uploadId: UUID
  ) {
    val multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId)
    val partsFolder = id?.let { getPartsFolder(bucket, uploadId) }

    check(
      multipartUploadInfo != null &&
        partsFolder != null &&
        partsFolder.toFile().exists() &&
        partsFolder.toFile().isDirectory
    ) {
      "Multipart Request was not prepared. bucket=$bucket, id=$id, uploadId=$uploadId, partsFolder=$partsFolder"
    }
  }

  private fun validateChecksums(
    uploadInfo: MultipartUploadInfo,
    tempFile: Path,
    completedParts: List<CompletedPart>,
    partsPaths: List<Path>,
    checksum: String?,
    checksumType: ChecksumType?,
    checksumAlgorithm: ChecksumAlgorithm?
  ): String? {
    val checksumToValidate = checksum ?: uploadInfo.checksum
    val checksumAlgorithmToValidate = checksumAlgorithm ?: uploadInfo.checksumAlgorithm
    if(checksumType != null && uploadInfo.checksumType != null && checksumType != uploadInfo.checksumType) {
      throw S3Exception.completeRequestWrongChecksumMode(uploadInfo.checksumType.name)
    }
    val checksumFor = if (uploadInfo.checksumType == ChecksumType.COMPOSITE) {
      checksumFor(partsPaths, uploadInfo)
    } else {
      checksumFor(tempFile, uploadInfo)
    }

    if (checksumAlgorithmToValidate != null) {
      completedParts.forEach { part ->
        if (part.checksum(checksumAlgorithmToValidate) == null) {
          throw S3Exception.completeRequestMissingChecksum(
            checksumAlgorithmToValidate.toString().lowercase(Locale.getDefault()),
            part.partNumber
          )
        }
      }
      if (checksumToValidate != null) {
        DigestUtil.verifyChecksum(checksumToValidate, checksumFor, checksumAlgorithmToValidate)
      }
    }

    return checksumFor
  }

  private fun checksumFor(paths: List<Path>, uploadInfo: MultipartUploadInfo): String? =
    uploadInfo.checksumAlgorithm?.let { algo ->
      DigestUtil.checksumMultipart(paths, algo.toChecksumAlgorithm())
    }

  private fun checksumFor(path: Path, uploadInfo: MultipartUploadInfo): String? =
    uploadInfo.checksumAlgorithm?.let { algo ->
      DigestUtil.checksumFor(path, algo.toChecksumAlgorithm())
    }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(MultipartStore::class.java)
    private const val PART_SUFFIX = ".part"
    private const val MULTIPART_UPLOAD_META_FILE = "multipartMetadata.json"
    const val MULTIPARTS_FOLDER: String = "multiparts"

    private fun toInputStream(paths: List<Path>): InputStream {
      val inputs = paths.map { path ->
        try {
          path.inputStream()
        } catch (e: IOException) {
          throw IllegalStateException("Can't access path $path", e)
        }
      }
      return SequenceInputStream(Collections.enumeration(inputs))
    }
  }
}
