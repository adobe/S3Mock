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
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Delete
import com.adobe.testing.s3mock.dto.DeleteResult
import com.adobe.testing.s3mock.dto.DeletedS3Object
import com.adobe.testing.s3mock.dto.Error
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.ObjectStore
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.DigestUtil.base64Digest
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit

open class ObjectService(private val bucketStore: BucketStore, private val objectStore: ObjectStore) : ServiceBase() {
  fun copyS3Object(
    sourceBucketName: String,
    sourceKey: String,
    versionId: String?,
    destinationBucketName: String,
    destinationKey: String,
    encryptionHeaders: Map<String, String>,
    storeHeaders: Map<String, String>,
    userMetadata: Map<String, String>,
    storageClass: StorageClass?
  ): S3ObjectMetadata? {
    val sourceBucketMetadata = bucketStore.getBucketMetadata(sourceBucketName)
    val destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucketName)
    val sourceId = sourceBucketMetadata.getID(sourceKey) ?: return null

    // source and destination is the same, pretend we copied - S3 does the same.
    if (sourceKey == destinationKey && sourceBucketName == destinationBucketName) {
      return objectStore.pretendToCopyS3Object(
        sourceBucketMetadata,
        sourceId,
        versionId,
        encryptionHeaders,
        storeHeaders,
        userMetadata,
        storageClass
      )
    }

    // source must be copied to destination
    val destinationId = bucketStore.addKeyToBucket(destinationKey, destinationBucketName)
    return try {
      objectStore.copyS3Object(
        sourceBucketMetadata, sourceId, versionId,
        destinationBucketMetadata, destinationId, destinationKey,
        encryptionHeaders, storeHeaders, userMetadata, storageClass
      )
    } catch (e: Exception) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucketName)
      throw e
    }
  }

  fun putS3Object(
    bucketName: String,
    key: String,
    contentType: String,
    storeHeaders: Map<String, String>,
    path: Path,
    userMetadata: Map<String, String>,
    encryptionHeaders: Map<String, String>,
    tags: List<Tag>?,
    checksumAlgorithm: ChecksumAlgorithm?,
    checksum: String?,
    owner: Owner,
    storageClass: StorageClass?
  ): S3ObjectMetadata {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key) ?: bucketStore.addKeyToBucket(key, bucketName)
    return objectStore.storeS3ObjectMetadata(
      bucketMetadata, id, key, contentType, storeHeaders,
      path, userMetadata, encryptionHeaders, null, tags,
      checksumAlgorithm, checksum, owner, storageClass, ChecksumType.FULL_OBJECT
    )
  }

  fun deleteObjects(bucketName: String, delete: Delete): DeleteResult {
    val deleted = mutableListOf<DeletedS3Object>()
    val errors = mutableListOf<Error>()
    delete.objectsToDelete.forEach { toDelete ->
      try {
        // ignore result of delete object.
        deleteObject(bucketName, toDelete.key, toDelete.versionId)
        // add deleted object even if it does not exist S3 does the same.
        deleted.add(DeletedS3Object.from(toDelete))
      } catch (e: IllegalStateException) {
        errors.add(
          Error(
            "InternalError",
            toDelete.key,
            "We encountered an internal error. Please try again.",
            toDelete.versionId
          )
        )
        LOG.error("Object could not be deleted!", e)
      }
    }
    return DeleteResult(deleted, errors)
  }

  fun deleteObject(bucketName: String, key: String, versionId: String?): Boolean {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val id = bucketMetadata.getID(key) ?: return false

    return if (objectStore.deleteObject(bucketMetadata, id, versionId)) {
      bucketStore.removeFromBucket(key, bucketName)
    } else {
      false
    }
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions).
   */
  fun setObjectTags(bucketName: String, key: String, versionId: String?, tags: List<Tag>?) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    objectStore.storeObjectTags(bucketMetadata, uuid, versionId, tags)
  }

  fun verifyObjectTags(tags: List<Tag>) {
    if (tags.size > MAX_ALLOWED_TAGS) throw S3Exception.INVALID_TAG
    verifyDuplicateTagKeys(tags)
    tags.forEach { tag ->
      verifyTagKeyPrefix(tag.key)
      verifyTagLength(MIN_ALLOWED_TAG_KEY_LENGTH, MAX_ALLOWED_TAG_KEY_LENGTH, tag.key)
      verifyTagChars(tag.key)

      verifyTagLength(MIN_ALLOWED_TAG_VALUE_LENGTH, MAX_ALLOWED_TAG_VALUE_LENGTH, tag.value)
      verifyTagChars(tag.value)
    }
  }

  private fun verifyDuplicateTagKeys(tags: List<Tag>) {
    val keys = mutableSetOf<String>()
    if (tags.any { !keys.add(it.key) }) throw S3Exception.INVALID_TAG
  }

  private fun verifyTagKeyPrefix(tagKey: String) {
    if (tagKey.startsWith(DISALLOWED_TAG_KEY_PREFIX)) throw S3Exception.INVALID_TAG
  }

  private fun verifyTagLength(minLength: Int, maxLength: Int, tag: String) {
    if (tag.length !in minLength..maxLength) throw S3Exception.INVALID_TAG
  }

  private fun verifyTagChars(tag: String) {
    if (!TAG_ALLOWED_CHARS.matches(tag)) throw S3Exception.INVALID_TAG
  }

  fun setLegalHold(bucketName: String, key: String, versionId: String?, legalHold: LegalHold) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    objectStore.storeLegalHold(bucketMetadata, uuid, versionId, legalHold)
  }

  fun setAcl(bucketName: String, key: String, versionId: String?, policy: AccessControlPolicy) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    objectStore.storeAcl(bucketMetadata, uuid, versionId, policy)
  }

  fun getAcl(bucketName: String, key: String, versionId: String?): AccessControlPolicy {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    return objectStore.readAcl(bucketMetadata, uuid, versionId)
  }

  fun setRetention(bucketName: String, key: String, versionId: String?, retention: Retention) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    objectStore.storeRetention(bucketMetadata, uuid, versionId, retention)
  }

  fun verifyRetention(retention: Retention) {
    if (Instant.now().isAfter(retention.retainUntilDate)) throw S3Exception.INVALID_REQUEST_RETAIN_DATE
  }

  fun verifyMd5(input: Path, contentMd5: String?) {
    try {
      input.toFile().inputStream().use {
        verifyMd5(it, contentMd5)
      }
    } catch (_: IOException) {
      throw S3Exception.BAD_REQUEST_CONTENT
    }
  }

  fun verifyMd5(inputStream: InputStream, contentMd5: String?) {
    contentMd5 ?: return
    val md5 = base64Digest(inputStream)
    if (md5 != contentMd5) {
      LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5)
      throw S3Exception.BAD_REQUEST_MD5
    }
  }

  /**
   * For copy use-cases, we need to return PRECONDITION_FAILED only.
   */
  fun verifyObjectMatchingForCopy(
    match: List<String>?,
    noneMatch: List<String>?,
    ifModifiedSince: List<Instant>?,
    ifUnmodifiedSince: List<Instant>?,
    s3ObjectMetadata: S3ObjectMetadata?
  ) {
    try {
      verifyObjectMatching(match, noneMatch, ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata)
    } catch (e: S3Exception) {
      if (e == S3Exception.NOT_MODIFIED) throw S3Exception.PRECONDITION_FAILED else throw e
    }
  }

  fun verifyObjectMatching(
    bucketName: String,
    key: String,
    match: List<String>?,
    noneMatch: List<String>?
  ) {
    try {
      val s3ObjectMetadataExisting = getObject(bucketName, key, null)
      verifyObjectMatching(match, noneMatch, null, null, s3ObjectMetadataExisting)
    } catch (e: S3Exception) {
      if (e === S3Exception.NOT_MODIFIED) throw S3Exception.PRECONDITION_FAILED else throw e
    }
  }

  fun verifyObjectMatching(
    match: List<String>?,
    matchLastModifiedTime: List<Instant>?,
    matchSize: List<Long>?,
    s3ObjectMetadata: S3ObjectMetadata?
  ) {
    verifyObjectMatching(match, null, null, null, s3ObjectMetadata)
    s3ObjectMetadata ?: return

    matchLastModifiedTime?.firstOrNull()?.let {
      val lastModified = Instant.ofEpochMilli(s3ObjectMetadata.lastModified)
      if (!lastModified.truncatedTo(ChronoUnit.SECONDS).equals(it.truncatedTo(ChronoUnit.SECONDS))) {
        throw S3Exception.PRECONDITION_FAILED
      }
    }

    matchSize?.firstOrNull()?.let {
      val size = s3ObjectMetadata.size.toLong()
      if (size != it) throw S3Exception.PRECONDITION_FAILED
    }
  }

  fun verifyObjectMatching(
    match: List<String>?,
    noneMatch: List<String>?,
    ifModifiedSince: List<Instant>?,
    ifUnmodifiedSince: List<Instant>?,
    s3ObjectMetadata: S3ObjectMetadata?
  ) {
    if (s3ObjectMetadata == null) {
      // object does not exist, client expects an existing object to match a value, but it could not be found.
      if (!match.isNullOrEmpty()) throw S3Exception.NO_SUCH_KEY
      // no client expectations, skip the rest of the checks.
      return
    }

    val etag = normalizeEtag(s3ObjectMetadata.etag)
    val lastModified = Instant.ofEpochMilli(s3ObjectMetadata.lastModified)

    ifModifiedSince?.firstOrNull()?.let {
      if (it.isAfter(lastModified)) {
        LOG.debug("Object {} not modified since {}", s3ObjectMetadata.key, it)
        throw S3Exception.NOT_MODIFIED
      }
    }

    if (!match.isNullOrEmpty()) {
      if (match.contains(WILDCARD_ETAG) || match.contains(WILDCARD) || match.contains(etag)) {
        // request cares only that the object exists or that the etag matches.
        LOG.debug("Object {} exists", s3ObjectMetadata.key)
        return
      } else if (!match.contains(etag)) {
        LOG.debug("Object {} does not match etag {}", s3ObjectMetadata.key, etag)
        throw S3Exception.PRECONDITION_FAILED
      }
    }

    ifUnmodifiedSince?.firstOrNull()?.let {
      if (it.isBefore(lastModified)) {
        LOG.debug("Object {} modified since {}", s3ObjectMetadata.key, it)
        throw S3Exception.PRECONDITION_FAILED
      }
    }

    if (!noneMatch.isNullOrEmpty()) {
      if (noneMatch.contains(WILDCARD_ETAG) || noneMatch.contains(WILDCARD) || noneMatch.contains(etag)) {
        // request cares only that the object etag does not match.
        LOG.debug("Object {} has an ETag {} that matches one of the 'noneMatch' values", s3ObjectMetadata.key, etag)
        throw S3Exception.NOT_MODIFIED
      }
    }
  }

  fun verifyObjectExists(bucketName: String, key: String, versionId: String?): S3ObjectMetadata {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: throw S3Exception.NO_SUCH_KEY
    val s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucketMetadata, uuid, versionId)
      ?: throw S3Exception.NO_SUCH_KEY
    if (s3ObjectMetadata.deleteMarker) throw S3Exception.NO_SUCH_KEY_DELETE_MARKER
    return s3ObjectMetadata
  }

  fun getObject(bucketName: String, key: String, versionId: String?): S3ObjectMetadata? {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val uuid = bucketMetadata.getID(key) ?: return null
    return objectStore.getS3ObjectMetadata(bucketMetadata, uuid, versionId)
  }

  fun verifyObjectLockConfiguration(bucketName: String, key: String, versionId: String?): S3ObjectMetadata {
    val s3ObjectMetadata = verifyObjectExists(bucketName, key, versionId)
    val noLegalHold = s3ObjectMetadata.legalHold == null
    val noRetention = s3ObjectMetadata.retention == null
    if (noLegalHold && noRetention) throw S3Exception.NOT_FOUND_OBJECT_LOCK
    return s3ObjectMetadata
  }

  companion object {
    const val WILDCARD_ETAG: String = "\"*\""
    const val WILDCARD: String = "*"
    private val LOG: Logger = LoggerFactory.getLogger(ObjectService::class.java)
    private val TAG_ALLOWED_CHARS: Regex = Regex("[\\w+ \\-=.:/@]*")
    private const val MAX_ALLOWED_TAGS = 50
    private const val MIN_ALLOWED_TAG_KEY_LENGTH = 1
    private const val MAX_ALLOWED_TAG_KEY_LENGTH = 128
    private const val MIN_ALLOWED_TAG_VALUE_LENGTH = 0
    private const val MAX_ALLOWED_TAG_VALUE_LENGTH = 256
    private const val DISALLOWED_TAG_KEY_PREFIX = "aws:"
  }
}
