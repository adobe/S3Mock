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
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.adobe.testing.s3mock.util.DigestUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import tools.jackson.databind.ObjectMapper
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists

open class ObjectStore(
  private val s3ObjectDateFormat: DateTimeFormatter,
  private val objectMapper: ObjectMapper
) : StoreBase() {
  /**
   * This map stores one lock object per S3Object ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private val lockStore: MutableMap<UUID, Any> = ConcurrentHashMap<UUID, Any>()

  fun storeS3ObjectMetadata(
    bucket: BucketMetadata,
    id: UUID,
    key: String,
    contentType: String?,
    storeHeaders: Map<String, String>?,
    path: Path,
    userMetadata: Map<String, String>?,
    encryptionHeaders: Map<String, String>?,
    etag: String?,
    tags: List<Tag>?,
    checksumAlgorithm: ChecksumAlgorithm?,
    checksum: String?,
    owner: Owner,
    storageClass: StorageClass?,
    checksumType: ChecksumType?
  ): S3ObjectMetadata {
    lockStore.putIfAbsent(id, Any())
    synchronized(lockStore[id]!!) {
      createObjectRootFolder(bucket, id)
      var versionId: String? = null
      if (bucket.isVersioningEnabled) {
        val existingVersions = getS3ObjectVersions(bucket, id)
        if (!existingVersions.versions.isEmpty()) {
          versionId = existingVersions.createVersion()
          writeVersionsFile(bucket, id, existingVersions)
        } else {
          val newVersions = createS3ObjectVersions(bucket, id)
          versionId = newVersions.createVersion()
          writeVersionsFile(bucket, id, newVersions)
        }
      }
      val dataFile = inputPathToFile(path, getDataFilePath(bucket, id, versionId))
      val now = Instant.now()
      val s3ObjectMetadata = S3ObjectMetadata(
        id,
        key,
        dataFile.length().toString(),
        s3ObjectDateFormat.format(now),
        etag
          ?: DigestUtil.hexDigest(
            encryptionHeaders!![AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID],
            dataFile
          ),
        contentType ?: MediaType.APPLICATION_OCTET_STREAM_VALUE,
        now.toEpochMilli(),
        dataFile.toPath(),
        userMetadata,
        tags,
        null,
        null,
        owner,
        storeHeaders,
        encryptionHeaders,
        checksumAlgorithm,
        checksum,
        storageClass,
        null,
        versionId,
        false,
        checksumType
      )
      writeMetafile(bucket, s3ObjectMetadata)
      return s3ObjectMetadata
    }
  }

  private fun privateCannedAcl(owner: Owner): AccessControlPolicy {
    val grant = Grant(
      CanonicalUser(null, owner.id),
      Grant.Permission.FULL_CONTROL
    )
    return AccessControlPolicy(listOf(grant), owner)
  }

  fun storeTags(bucket: BucketMetadata, id: UUID, versionId: String?, tags: List<Tag>?) {
    synchronized(lockStore[id]!!) {
      val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
      writeMetafile(bucket, s3ObjectMetadata!!.copy(tags = tags))
    }
  }

  fun storeLegalHold(bucket: BucketMetadata, id: UUID, versionId: String?, legalHold: LegalHold) {
    synchronized(lockStore[id]!!) {
      val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
      writeMetafile(bucket, s3ObjectMetadata!!.copy(legalHold = legalHold))
    }
  }

  fun storeAcl(bucket: BucketMetadata, id: UUID, versionId: String?, policy: AccessControlPolicy) {
    synchronized(lockStore[id]!!) {
      val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
      writeMetafile(bucket, s3ObjectMetadata!!.copy(policy = policy))
    }
  }

  fun readAcl(bucket: BucketMetadata, id: UUID, versionId: String?): AccessControlPolicy {
    val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
    return (s3ObjectMetadata?.policy ?: privateCannedAcl(s3ObjectMetadata!!.owner))
  }

  fun storeRetention(bucket: BucketMetadata, id: UUID, versionId: String?, retention: Retention) {
    synchronized(lockStore[id]!!) {
      val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
      writeMetafile(bucket, s3ObjectMetadata!!.copy(retention = retention))
    }
  }

  fun getS3ObjectMetadata(bucket: BucketMetadata, id: UUID, versionId: String?): S3ObjectMetadata? {
    var versionId = versionId
    if (bucket.isVersioningEnabled && versionId == null) {
      val s3ObjectVersions = getS3ObjectVersions(bucket, id)
      versionId = s3ObjectVersions.latestVersion
    }
    val metaPath = getMetaFilePath(bucket, id, versionId)

    if (metaPath.exists()) {
      synchronized(lockStore[id]!!) {
        try {
          return objectMapper.readValue(metaPath.toFile(), S3ObjectMetadata::class.java)
        } catch (e: IOException) {
          throw IllegalArgumentException("Could not read object metadata-file $id", e)
        }
      }
    }
    return null
  }

  fun getS3ObjectVersions(bucket: BucketMetadata, id: UUID): S3ObjectVersions {
    val metaPath = getVersionFilePath(bucket, id)

    if (metaPath.exists()) {
      synchronized(lockStore[id]!!) {
        try {
          return objectMapper.readValue(metaPath.toFile(), S3ObjectVersions::class.java)
        } catch (e: IOException) {
          throw IllegalArgumentException("Could not read object versions-file $id", e)
        }
      }
    }
    return S3ObjectVersions.empty(id)
  }

  fun createS3ObjectVersions(bucket: BucketMetadata, id: UUID): S3ObjectVersions {
    val metaPath = getVersionFilePath(bucket, id)

    if (metaPath.exists()) {
      // gracefully handle duplicate version creation
      return getS3ObjectVersions(bucket, id)
    } else {
      synchronized(lockStore[id]!!) {
        try {
          writeVersionsFile(bucket, id, S3ObjectVersions(id))
          return objectMapper.readValue(metaPath.toFile(), S3ObjectVersions::class.java)
        } catch (e: IOException) {
          throw IllegalArgumentException("Could not read object versions-file $id", e)
        }
      }
    }
  }

  fun copyObject(
    sourceBucket: BucketMetadata,
    sourceId: UUID,
    versionId: String?,
    destinationBucket: BucketMetadata,
    destinationId: UUID,
    destinationKey: String,
    encryptionHeaders: Map<String, String>?,
    storeHeaders: Map<String, String>?,
    userMetadata: Map<String, String>?,
    storageClass: StorageClass?
  ): S3ObjectMetadata? {
    val sourceObject = getS3ObjectMetadata(sourceBucket, sourceId, versionId) ?: return null
    synchronized(lockStore[sourceId]!!) {
      return storeS3ObjectMetadata(
        destinationBucket,
        destinationId,
        destinationKey,
        sourceObject.contentType,
        if (storeHeaders == null || storeHeaders.isEmpty())
          sourceObject.storeHeaders
        else
          storeHeaders,
        sourceObject.dataPath,
        if (userMetadata == null || userMetadata.isEmpty())
          sourceObject.userMetadata
        else
          userMetadata,
        if (encryptionHeaders == null || encryptionHeaders.isEmpty())
          sourceObject.encryptionHeaders
        else
          encryptionHeaders,
        null,
        sourceObject.tags,
        sourceObject.checksumAlgorithm,
        sourceObject.checksum,
        sourceObject.owner,
        storageClass ?: sourceObject.storageClass,
        sourceObject.checksumType
      )
    }
  }

  /**
   * If source and destination is the same, pretend we copied - S3 does the same.
   * This does not change the modificationDate.
   * Also, this would need to increment the version if/when we support versioning.
   */
  fun pretendToCopyObject(
    sourceBucket: BucketMetadata,
    sourceId: UUID,
    versionId: String?,
    encryptionHeaders: Map<String, String>?,
    storeHeaders: Map<String, String>?,
    userMetadata: Map<String, String>?,
    storageClass: StorageClass?
  ): S3ObjectMetadata? {
    val sourceObject = getS3ObjectMetadata(sourceBucket, sourceId, versionId) ?: return null

    verifyPretendCopy(sourceObject, userMetadata, encryptionHeaders, storeHeaders, storageClass)

    val s3ObjectMetadata = sourceObject.copy(
      lastModified = Instant.now().toEpochMilli(),
      userMetadata = if (userMetadata == null || userMetadata.isEmpty())
        sourceObject.userMetadata
      else
        userMetadata,
      encryptionHeaders = if (encryptionHeaders == null || encryptionHeaders.isEmpty())
        sourceObject.encryptionHeaders
      else
        encryptionHeaders,
      storeHeaders = if (storeHeaders == null || storeHeaders.isEmpty())
        sourceObject.storeHeaders
      else
        storeHeaders,
      storageClass = storageClass ?: sourceObject.storageClass,
    )
    writeMetafile(sourceBucket, s3ObjectMetadata)
    return s3ObjectMetadata
  }

  fun deleteObject(
    bucket: BucketMetadata,
    id: UUID,
    versionId: String?
  ): Boolean {
    val s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId)
    if (s3ObjectMetadata != null) {
      if (bucket.isVersioningEnabled && "null" != versionId) {
        return if (versionId != null) {
          doDeleteVersion(bucket, id, versionId)
        } else {
          insertDeleteMarker(bucket, id, s3ObjectMetadata)
        }
      }
      return doDeleteObject(bucket, id)
    } else {
      return false
    }
  }

  /**
   * Deletes a specific version of an object, if found.
   * If this is the last version of an object, it deletes the object.
   * Returns true if the *LAST* version was deleted.
   */
  private fun doDeleteVersion(bucket: BucketMetadata, id: UUID, versionId: String): Boolean {
    synchronized(lockStore[id]!!) {
      try {
        val existingVersions = getS3ObjectVersions(bucket, id)
        if (existingVersions.versions.isEmpty()) {
          // no versions exist, nothing to delete.
          return false
        }
        if (existingVersions.versions.size == 1) {
          // this is the last version of an object, delete object completely.
          return doDeleteObject(bucket, id)
        } else {
          // there is at least one version of an object left, delete only the version.
          existingVersions.deleteVersion(versionId)
          writeVersionsFile(bucket, id, existingVersions)
          return false
        }
      } catch (e: Exception) {
        throw IllegalStateException("Could not delete object-version $id", e)
      }
    }
  }

  fun doDeleteObject(bucket: BucketMetadata, id: UUID): Boolean {
    synchronized(lockStore[id]!!) {
      try {
        getObjectFolderPath(bucket, id).toFile().deleteRecursively()
      } catch (e: IOException) {
        throw IllegalStateException("Could not delete object-directory $id", e)
      }
      lockStore.remove(id)
      return true
    }
  }

  /**
   * See [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html).
   */
  private fun insertDeleteMarker(
    bucket: BucketMetadata,
    id: UUID,
    s3ObjectMetadata: S3ObjectMetadata
  ): Boolean {
    var versionId: String? = null
    synchronized(lockStore[id]!!) {
      try {
        val existingVersions = getS3ObjectVersions(bucket, id)
        if (!existingVersions.versions.isEmpty()) {
          versionId = existingVersions.createVersion()
          writeVersionsFile(bucket, id, existingVersions)
        }
        writeMetafile(bucket, S3ObjectMetadata.deleteMarker(s3ObjectMetadata, versionId))
      } catch (e: Exception) {
        throw IllegalStateException("Could not insert object-deletemarker $id", e)
      }
      return false
    }
  }

  /**
   * Used to load metadata for all objects from a bucket when S3Mock starts.
   *
   * @param bucketMetadata metadata of existing bucket.
   * @param ids ids of the keys to load
   */
  fun loadObjects(bucketMetadata: BucketMetadata, ids: Collection<UUID>) {
    var loaded = 0
    for (id in ids) {
      lockStore.putIfAbsent(id, Any())
      val s3ObjectVersions = getS3ObjectVersions(bucketMetadata, id)
      if (!s3ObjectVersions.versions.isEmpty()) {
        if (loadVersions(bucketMetadata, s3ObjectVersions)) {
          loaded++
        }
      } else {
        val s3ObjectMetadata = getS3ObjectMetadata(bucketMetadata, id, null)
        if (s3ObjectMetadata != null) {
          loaded++
        }
      }
    }
    LOG.info("Loaded {}/{} objects for bucket {}", loaded, ids.size, bucketMetadata.name)
  }

  private fun loadVersions(bucket: BucketMetadata, versions: S3ObjectVersions): Boolean {
    var loaded = false
    val s3ObjectVersions = getS3ObjectVersions(bucket, versions.id)
    for (version in s3ObjectVersions.versions) {
      val s3ObjectMetadata = getS3ObjectMetadata(bucket, versions.id, version)
      if (s3ObjectMetadata != null) {
        loaded = true
      }
    }
    return loaded
  }

  private fun createObjectRootFolder(bucket: BucketMetadata, id: UUID) {
    val objectRootFolder = getObjectFolderPath(bucket, id).toFile()
    objectRootFolder.mkdirs()
  }

  private fun getObjectFolderPath(bucket: BucketMetadata, id: UUID): Path {
    return Paths.get(bucket.path.toString(), id.toString())
  }

  private fun getMetaFilePath(bucket: BucketMetadata, id: UUID, versionId: String?): Path {
    if (versionId != null && NULL_VERSION != versionId) {
      return getObjectFolderPath(bucket, id).resolve("${versionId}$VERSIONED_META_FILE")
    }
    return getObjectFolderPath(bucket, id).resolve(META_FILE)
  }

  private fun getDataFilePath(bucket: BucketMetadata, id: UUID, versionId: String?): Path {
    if (versionId != null) {
      return getObjectFolderPath(bucket, id).resolve("${versionId}$VERSIONED_DATA_FILE")
    }
    return getObjectFolderPath(bucket, id).resolve(DATA_FILE)
  }

  private fun getVersionFilePath(bucket: BucketMetadata, id: UUID): Path {
    return getObjectFolderPath(bucket, id).resolve(VERSIONS_FILE)
  }

  private fun writeVersionsFile(
    bucket: BucketMetadata, id: UUID,
    s3ObjectVersions: S3ObjectVersions
  ) {
    try {
      synchronized(lockStore[id]!!) {
        val versionsFile = getVersionFilePath(bucket, id).toFile()
        objectMapper.writeValue(versionsFile, s3ObjectVersions)
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write object versions-file $id", e)
    }
  }

  private fun writeMetafile(bucket: BucketMetadata, s3ObjectMetadata: S3ObjectMetadata) {
    val id = s3ObjectMetadata.id
    try {
      synchronized(lockStore[id]!!) {
        val metaFile = getMetaFilePath(bucket, id, s3ObjectMetadata.versionId).toFile()
        objectMapper.writeValue(metaFile, s3ObjectMetadata)
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write object metadata-file $id", e)
    }
  }

  private fun verifyPretendCopy(
    sourceObject: S3ObjectMetadata,
    userMetadata: Map<String, String>?,
    encryptionHeaders: Map<String, String>?,
    storeHeaders: Map<String, String>?,
    storageClass: StorageClass?
  ) {
    val userDataUnChanged = userMetadata == null || userMetadata.isEmpty()
    val encryptionHeadersUnChanged = encryptionHeaders == null || encryptionHeaders.isEmpty()
    val storeHeadersUnChanged = storeHeaders == null || storeHeaders.isEmpty()
    val storageClassUnChanged = storageClass == null || storageClass == sourceObject.storageClass
    if (userDataUnChanged
      && storageClassUnChanged
      && encryptionHeadersUnChanged
      && storeHeadersUnChanged
    ) {
      throw S3Exception.INVALID_COPY_REQUEST_SAME_KEY
    }
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(ObjectStore::class.java)
    private const val META_FILE = "objectMetadata.json"
    private const val DATA_FILE = "binaryData"
    private const val VERSIONED_META_FILE = "-objectMetadata.json"
    private const val VERSIONED_DATA_FILE = "-binaryData"
    private const val VERSIONS_FILE = "versions.json"

    // if a bucket isn't version enabled, some APIs return "null" as the versionId for objects.
    // clients may also pass in "null" as a version, expecting the behaviour for non-versioned objects.
    private const val NULL_VERSION = "null"
  }
}
