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

import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled.ENABLED
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

/**
 * Stores buckets and their metadata created in S3Mock.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-buckets-s3.html)
 */
open class BucketStore(
  private val rootFolder: File,
  private val s3ObjectDateFormat: DateTimeFormatter,
  private val region: String,
  private val objectMapper: ObjectMapper
) {
  /**
   * This map stores one lock object per Bucket name.
   * Any method modifying the underlying file must aquire the lock object before the modification.
   */
  private val lockStore: MutableMap<String, Any> = ConcurrentHashMap<String, Any>()

  fun listBuckets(): List<BucketMetadata> {
    return findBucketPaths()
      .map { it.fileName.toString() }
      .map { getBucketMetadata(it) }
  }

  fun getBucketMetadata(bucketName: String): BucketMetadata {
    try {
      check(doesBucketExist(bucketName)) { "Bucket does not exist: $bucketName" }
      val metaFilePath = getMetaFilePath(bucketName)
      synchronized(lockStore[bucketName]!!) {
        return objectMapper.readValue(metaFilePath.toFile(), BucketMetadata::class.java)
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not read bucket metadata-file $bucketName", e)
    }
  }

  @Synchronized
  fun addKeyToBucket(key: String, bucketName: String): UUID {
    synchronized(lockStore[bucketName]!!) {
      val bucketMetadata = getBucketMetadata(bucketName)
      val uuid = bucketMetadata.addKey(key)
      writeToDisk(bucketMetadata)
      return uuid
    }
  }

  fun lookupIdsInBucket(prefix: String?, bucketName: String): List<UUID> {
    return lookupInBucket(prefix, bucketName) { it.value }
  }

  fun lookupKeysInBucket(prefix: String?, bucketName: String): List<String> {
    return lookupInBucket(prefix, bucketName) { it.key }
  }

  private fun <R> lookupInBucket(
    prefix: String?,
    bucketName: String,
    extract: (MutableMap.MutableEntry<String, UUID>) -> R
  ): List<R> {
    val normalizedPrefix = prefix ?: ""
    synchronized(lockStore[bucketName]!!) {
      return getBucketMetadata(bucketName).objects
        .entries
        .asSequence()
        .filter { it.key.startsWith(normalizedPrefix) }
        .map(extract)
        .toList()
    }
  }

  @Synchronized
  fun removeFromBucket(key: String, bucketName: String): Boolean {
    synchronized(lockStore.get(bucketName)!!) {
      val bucketMetadata = getBucketMetadata(bucketName)
      val removed = bucketMetadata.removeKey(key)
      writeToDisk(bucketMetadata)
      return removed
    }
  }

  private fun findBucketPaths(): List<Path> =
    try {
      rootFolder
        .toPath()
        .listDirectoryEntries()
        .filter { it.isDirectory() }
    } catch (e: IOException) {
      throw IllegalStateException("Could not Iterate over Bucket-Folders.", e)
    }

  fun createBucket(
    bucketName: String,
    objectLockEnabled: Boolean,
    objectOwnership: ObjectOwnership,
    bucketRegion: String?,
    bucketInfo: BucketInfo?,
    locationInfo: LocationInfo?
  ): BucketMetadata {
    check(!doesBucketExist(bucketName)) { "Bucket already exists." }
    lockStore.putIfAbsent(bucketName, Any())
    synchronized(lockStore[bucketName]!!) {
      val bucketFolder = createBucketFolder(bucketName)
      val region = bucketRegion ?: this.region

      val newBucketMetadata = BucketMetadata(
        bucketName,
        s3ObjectDateFormat.format(LocalDateTime.now()),
        VersioningConfiguration(null, null),
        if (objectLockEnabled) ObjectLockConfiguration(ENABLED, null) else null,
        null,
        objectOwnership,
        bucketFolder.toPath(),
        region,
        bucketInfo,
        locationInfo
      )
      writeToDisk(newBucketMetadata)
      return newBucketMetadata
    }
  }

  /**
   * Checks if the specified bucket exists. Amazon S3 buckets are named in a global namespace; use
   * this method to determine if a specified bucket name already exists, and therefore can't be used
   * to create a new bucket.
   */
  fun doesBucketExist(bucketName: String): Boolean {
    val metaFilePath = getMetaFilePath(bucketName)
    return metaFilePath.toFile().exists()
  }

  fun isObjectLockEnabled(bucketName: String): Boolean {
    val objectLockConfiguration = getBucketMetadata(bucketName).objectLockConfiguration
    return objectLockConfiguration?.objectLockEnabled == ENABLED
  }

  fun storeObjectLockConfiguration(
    metadata: BucketMetadata,
    configuration: ObjectLockConfiguration
  ) {
    synchronized(lockStore[metadata.name]!!) {
      writeToDisk(metadata.withObjectLockConfiguration(configuration))
    }
  }

  fun storeVersioningConfiguration(
    metadata: BucketMetadata,
    configuration: VersioningConfiguration
  ) {
    synchronized(lockStore[metadata.name]!!) {
      writeToDisk(metadata.withVersioningConfiguration(configuration))
    }
  }

  fun storeBucketLifecycleConfiguration(
    metadata: BucketMetadata,
    configuration: BucketLifecycleConfiguration?
  ) {
    synchronized(lockStore[metadata.name]!!) {
      writeToDisk(metadata.withBucketLifecycleConfiguration(configuration))
    }
  }

  fun isBucketEmpty(bucketName: String): Boolean {
    check(doesBucketExist(bucketName)) { "Requested Bucket does not exist: $bucketName" }
    return getBucketMetadata(bucketName).objects.isEmpty()
  }

  fun deleteBucket(bucketName: String): Boolean {
    try {
      synchronized(lockStore.get(bucketName)!!) {
        return if (isBucketEmpty(bucketName)) {
          val bucketMetadata = getBucketMetadata(bucketName)
          bucketMetadata.path.toFile().deleteRecursively()
          lockStore.remove(bucketName)
          true
        } else {
          false
        }
      }
    } catch (e: IOException) {
      throw IllegalStateException("Can't delete bucket directory!", e)
    }
  }

  /**
   * Used to load metadata for all buckets when S3Mock starts.
   */
  fun loadBuckets(bucketNames: List<String>): List<UUID> {
    val objectIds = mutableListOf<UUID>()
    for (bucketName in bucketNames) {
      LOG.info("Loading existing bucket {}.", bucketName)
      lockStore.putIfAbsent(bucketName, Any())
      synchronized(lockStore[bucketName]!!) {
        val bucketMetadata = getBucketMetadata(bucketName)
        for ((key, value) in bucketMetadata.objects.entries) {
          objectIds.add(value)
          LOG.info("Loading existing bucket {} key {}", bucketName, key)
        }
      }
    }
    return objectIds
  }

  private fun writeToDisk(bucketMetadata: BucketMetadata) {
    try {
      val metaFile = getMetaFilePath(bucketMetadata.name).toFile()
      synchronized(lockStore[bucketMetadata.name]!!) {
        objectMapper.writeValue(metaFile, bucketMetadata)
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write bucket metadata-file", e)
    }
  }

  private fun getBucketFolderPath(bucketName: String): Path {
    val rootPath = rootFolder.toPath().toAbsolutePath().normalize()
    val candidate = rootPath.resolve(bucketName).normalize()
    require(candidate.startsWith(rootPath)) { "Invalid bucket name (path traversal detected)." }
    return candidate
  }

  private fun createBucketFolder(bucketName: String): File {
    try {
      val bucketFolder = getBucketFolderPath(bucketName).toFile()
      bucketFolder.mkdirs()
      return bucketFolder
    } catch (e: IOException) {
      throw IllegalStateException("Can't create bucket directory!", e)
    }
  }

  private fun getMetaFilePath(bucketName: String): Path {
    return getBucketFolderPath(bucketName).resolve(BUCKET_META_FILE)
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(BucketStore::class.java)
    const val BUCKET_META_FILE: String = "bucketMetadata.json"
  }
}
