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
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.DigestUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.stream.Streams
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpRange
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.SequenceInputStream
import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Collections
import java.util.Date
import java.util.Locale
import java.util.Objects
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import java.util.stream.StreamSupport

open class MultipartStore(private val objectStore: ObjectStore, private val objectMapper: ObjectMapper) : StoreBase() {
    /**
     * This map stores one lock object per MultipartUpload ID.
     * Any method modifying the underlying file must acquire the lock object before the modification.
     */
    private val lockStore: MutableMap<UUID, Any> = ConcurrentHashMap<UUID, Any>()

    fun createMultipartUpload(
        bucket: BucketMetadata,
        key: String,
        id: UUID,
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
            checksumAlgorithm,
            false
        )
        lockStore.putIfAbsent(uploadId, Any())
        writeMetafile(bucket, multipartUploadInfo)

        return upload
    }

    fun listMultipartUploads(bucketMetadata: BucketMetadata, prefix: String?): MutableList<MultipartUpload> {
        val multipartsFolder = getMultipartsFolder(bucketMetadata)
        if (!multipartsFolder.toFile().exists()) {
            return mutableListOf()
        }
        try {
            Files.newDirectoryStream(multipartsFolder).use { paths ->
                return Streams
                    .of(paths)
                    .map<MultipartUploadInfo?> { path: Path? ->
                        val fileName = path!!.getFileName().toString()
                        getUploadMetadata(bucketMetadata, UUID.fromString(fileName))
                    }
                    .filter { obj: MultipartUploadInfo? -> Objects.nonNull(obj) }
                    .filter { uploadMetadata: MultipartUploadInfo? -> !uploadMetadata!!.completed }
                    .map(MultipartUploadInfo::upload)
                    .filter { upload: MultipartUpload? -> StringUtils.isBlank(prefix) || upload!!.key.startsWith(prefix!!) }
                    .toList()
            }
        } catch (e: IOException) {
            throw IllegalStateException("Could not load buckets from data directory ", e)
        }
    }

    fun getMultipartUploadInfo(
        bucketMetadata: BucketMetadata,
        uploadId: UUID
    ): MultipartUploadInfo? {
        return getUploadMetadata(bucketMetadata, uploadId)
    }

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
                    FileUtils.deleteDirectory(getPartsFolder(bucket, uploadId).toFile())
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
        checksumAlgorithm: ChecksumAlgorithm?
    ): CompleteMultipartUploadResult {
        requireNotNull(uploadInfo) { "Unknown upload $uploadId" }
        val partFolder = getPartsFolder(bucket, uploadId)
        val partsPaths =
            parts
                .stream()
                .map<Path> { part: CompletedPart? ->
                    Paths.get(
                        partFolder.toString(),
                        part!!.partNumber.toString() + PART_SUFFIX
                    )
                }
                .toList()
        var tempFile: Path? = null
        try {
            tempFile = Files.createTempFile("completeMultipartUpload", "")

            toInputStream(partsPaths).use { `is` ->
                Files.newOutputStream(tempFile).use { os ->
                    `is`.transferTo(os)
                    val checksumFor = validateChecksums(uploadInfo, parts, partsPaths, checksum, checksumAlgorithm)
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
                        ChecksumType.COMPOSITE
                    )
                    // delete parts and update MultipartInfo
                    partsPaths.forEach(Consumer { partPath: Path? -> FileUtils.deleteQuietly(partPath!!.toFile()) })
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
                }
            }
        } catch (e: IOException) {
            throw IllegalStateException(
                String.format(
                    "Error finishing multipart upload bucket=%s, key=%s, id=%s, uploadId=%s",
                    bucket, key, id, uploadId
                ), e
            )
        } finally {
          tempFile?.toFile()?.delete()
        }
    }

    private fun validateChecksums(
        uploadInfo: MultipartUploadInfo,
        completedParts: List<CompletedPart>,
        partsPaths: List<Path>,
        checksum: String?,
        checksumAlgorithm: ChecksumAlgorithm?
    ): String? {
        val checksumToValidate = checksum ?: uploadInfo.checksum
        val checksumAlgorithmToValidate = checksumAlgorithm ?: uploadInfo.checksumAlgorithm
        val checksumFor = checksumFor(partsPaths, uploadInfo)
        if (checksumAlgorithmToValidate != null) {
            for (part in completedParts) {
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

    private fun checksumFor(paths: List<Path>, uploadInfo: MultipartUploadInfo): String? {
        if (uploadInfo.checksumAlgorithm != null) {
            return DigestUtil.checksumMultipart(
                paths,
                uploadInfo.checksumAlgorithm.toChecksumAlgorithm()
            )
        }
        return null
    }

    fun getMultipartUploadParts(
        bucket: BucketMetadata,
        id: UUID,
        uploadId: UUID
    ): List<Part> {
        val partsPath = getPartsFolder(bucket, uploadId)
        try {
            Files.newDirectoryStream(
                partsPath,
                DirectoryStream.Filter { path: Path? -> path!!.fileName.toString().endsWith(PART_SUFFIX) })
                .use { directoryStream ->
                    return StreamSupport.stream(directoryStream.spliterator(), false)
                        .map { path: Path? ->
                            val name = path!!.fileName.toString()
                            val prefix = name.substringBefore('.')
                            val partNumber = prefix.toInt()
                            val partMd5 = DigestUtil.hexDigest(path.toFile())
                            val lastModified = Date(path.toFile().lastModified())
                            Part(partNumber, partMd5, lastModified, path.toFile().length())
                        }
                        .sorted(Comparator.comparing(Part::partNumber))
                        .toList()
                }
        } catch (e: IOException) {
            throw IllegalStateException(
                String.format(
                    "Could not read all parts. "
                            + "bucket=%s, id=%s, uploadId=%s", bucket, id, uploadId
                ), e
            )
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
        val s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucket, id, versionId)
        var len = s3ObjectMetadata!!.dataPath.toFile().length()
        if (copyRange != null) {
            from = copyRange.getRangeStart(len)
            len = copyRange.getRangeEnd(len) - copyRange.getRangeStart(len) + 1
        }

        try {
            FileUtils.openInputStream(s3ObjectMetadata.dataPath.toFile()).use { sourceStream ->
                Files.newOutputStream(partFile.toPath()).use { targetStream ->
                    val skip = sourceStream.skip(from)
                    if (skip == from) {
                        BoundedInputStream
                            .builder()
                            .setInputStream(sourceStream)
                            .setMaxCount(len)
                            .get().use { bis ->
                                bis.transferTo(targetStream)
                            }
                    } else {
                        throw IllegalStateException("Could not skip exact byte range")
                    }
                }
            }
        } catch (e: IOException) {
            throw IllegalStateException(
                String.format(
                    "Could not copy object. "
                            + "bucket=%s, id=%s, range=%s, partFile=%s", bucket, id, copyRange, partFile
                ), e
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
            check(!(!partFile.exists() && !partFile.createNewFile())) {
                String.format(
                    "Could not create buffer file. "
                            + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber
                )
            }
        } catch (e: IOException) {
            throw IllegalStateException(
                String.format(
                    "Could not create buffer file. "
                            + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber
                ), e
            )
        }
        return partFile
    }

    private fun verifyMultipartUploadPreparation(
        bucket: BucketMetadata,
        id: UUID?,
        uploadId: UUID
    ) {
        var partsFolder: Path? = null
        val multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId)
        if (id != null) {
            partsFolder = getPartsFolder(bucket, uploadId)
        }

        check(
            !(multipartUploadInfo == null || partsFolder == null || !partsFolder.toFile()
                .exists() || !partsFolder.toFile().isDirectory())
        ) {
            String.format(
                "Multipart Request was not prepared. bucket=%s, id=%s, uploadId=%s, partsFolder=%s",
                bucket, id, uploadId, partsFolder
            )
        }
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

        if (Files.exists(metaPath)) {
            synchronized(lockStore.get(uploadId)!!) {
                try {
                    return objectMapper.readValue<MultipartUploadInfo>(
                        metaPath.toFile(),
                        MultipartUploadInfo::class.java
                    )
                } catch (e: IOException) {
                    throw IllegalArgumentException("Could not read upload metadata-file " + uploadId, e)
                }
            }
        }
        return null
    }

    private fun writeMetafile(bucket: BucketMetadata, uploadInfo: MultipartUploadInfo) {
        val uploadId = uploadInfo.upload.uploadId
        try {
            synchronized(lockStore.get(UUID.fromString(uploadId))!!) {
                val metaFile = getUploadMetadataPath(bucket, UUID.fromString(uploadId)).toFile()
                objectMapper.writeValue(metaFile, uploadInfo)
            }
        } catch (e: IOException) {
            throw IllegalStateException("Could not write upload metadata-file " + uploadId, e)
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(MultipartStore::class.java)
        private const val PART_SUFFIX = ".part"
        private const val MULTIPART_UPLOAD_META_FILE = "multipartMetadata.json"
        const val MULTIPARTS_FOLDER: String = "multiparts"

        private fun toInputStream(paths: MutableList<Path>): InputStream {
            val result = ArrayList<InputStream>()
            for (path in paths) {
                try {
                    result.add(Files.newInputStream(path))
                } catch (e: IOException) {
                    throw IllegalStateException("Can't access path " + path, e)
                }
            }
            return SequenceInputStream(Collections.enumeration<InputStream>(result))
        }
    }
}
