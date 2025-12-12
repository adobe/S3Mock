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
import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.BucketType
import com.adobe.testing.s3mock.dto.Buckets
import com.adobe.testing.s3mock.dto.DeleteMarkerEntry
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.dto.ListBucketResult
import com.adobe.testing.s3mock.dto.ListBucketResultV2
import com.adobe.testing.s3mock.dto.ListVersionsResult
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.ObjectVersion
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Prefix
import com.adobe.testing.s3mock.dto.Region
import com.adobe.testing.s3mock.dto.S3Object
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.BucketStore
import com.adobe.testing.s3mock.store.ObjectStore
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_NAME
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_TYPE
import software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.let

open class BucketService(
  private val bucketStore: BucketStore,
  private val objectStore: ObjectStore
) : ServiceBase() {
  private val listObjectsPagingStateCache: MutableMap<String, String> = ConcurrentHashMap()
  private val listBucketsPagingStateCache: MutableMap<String, String> = ConcurrentHashMap()

  fun isBucketEmpty(bucketName: String): Boolean {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val objects = bucketMetadata.objects
    if (objects.isEmpty()) return true

    return objects.values.none { id ->
      objectStore.getS3ObjectMetadata(bucketMetadata, id, null)?.deleteMarker == false
    }
  }

  fun doesBucketExist(bucketName: String): Boolean =
    bucketStore.doesBucketExist(bucketName)

  fun listBuckets(
    bucketRegion: Region?,
    continuationToken: String?,
    maxBuckets: Int,
    prefix: String?
  ): ListAllMyBucketsResult {
    val normalizedPrefix = prefix.orEmpty()

    var buckets = bucketStore
      .listBuckets()
      .filter { it.name.startsWith(normalizedPrefix) }
      .sortedBy { it.name }
      .map { Bucket.from(it) }

    bucketRegion?.let {
      buckets = buckets.filter { it.bucketRegion == it.toString() }
    }

    var nextContinuationToken: String? = null

    continuationToken?.let {
      val continueAfter = listBucketsPagingStateCache.remove(it)
      buckets = filterBy(buckets, Bucket::name, continueAfter)
    }

    if (buckets.size > maxBuckets) {
      nextContinuationToken = UUID.randomUUID().toString()
      buckets = buckets.subList(0, maxBuckets)
      buckets[maxBuckets - 1].name?.let {
        listBucketsPagingStateCache[nextContinuationToken] = it
      }
    }

    return ListAllMyBucketsResult(Owner.DEFAULT_OWNER, Buckets(buckets), prefix, nextContinuationToken)
  }

  fun getBucket(bucketName: String): Bucket =
    Bucket.from(bucketStore.getBucketMetadata(bucketName))

  fun createBucket(
    bucketName: String,
    objectLockEnabled: Boolean,
    objectOwnership: ObjectOwnership,
    bucketRegion: String?,
    bucketInfo: BucketInfo?,
    locationInfo: LocationInfo?
  ): Bucket =
    Bucket.from(
      bucketStore.createBucket(
        bucketName,
        objectLockEnabled,
        objectOwnership,
        bucketRegion,
        bucketInfo,
        locationInfo
      )
    )

  fun deleteBucket(bucketName: String): Boolean {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val objects = bucketMetadata.objects

    if (objects.isNotEmpty()) {
      // snapshot to avoid concurrent modification while removing from store
      for ((key, id) in objects.toList()) {
        val meta = objectStore.getS3ObjectMetadata(bucketMetadata, id, null)
        if (meta?.deleteMarker == true) {
          // delete-marker objects "do not officially exist"
          objectStore.doDeleteObject(bucketMetadata, id)
          bucketStore.removeFromBucket(key, bucketName)
        }
      }
    }

    bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    check(bucketMetadata.objects.isEmpty()) { "Bucket is not empty: $bucketName" }
    return bucketStore.deleteBucket(bucketName)
  }

  fun setVersioningConfiguration(bucketName: String, configuration: VersioningConfiguration) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    bucketStore.storeVersioningConfiguration(bucketMetadata, configuration)
  }

  fun getVersioningConfiguration(bucketName: String): VersioningConfiguration {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    return bucketMetadata.versioningConfiguration ?: throw S3Exception.NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION
  }

  fun setObjectLockConfiguration(bucketName: String, configuration: ObjectLockConfiguration) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    bucketStore.storeObjectLockConfiguration(bucketMetadata, configuration)
  }

  fun getObjectLockConfiguration(bucketName: String): ObjectLockConfiguration {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    return bucketMetadata.objectLockConfiguration ?: throw S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK
  }

  fun setBucketLifecycleConfiguration(
    bucketName: String,
    configuration: BucketLifecycleConfiguration?
  ) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    bucketStore.storeBucketLifecycleConfiguration(bucketMetadata, configuration)
  }

  fun deleteBucketLifecycleConfiguration(bucketName: String) {
    setBucketLifecycleConfiguration(bucketName, null)
  }

  fun getBucketLifecycleConfiguration(bucketName: String): BucketLifecycleConfiguration {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    return bucketMetadata.bucketLifecycleConfiguration ?: throw S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION
  }

  fun getS3Objects(bucketName: String, prefix: String?): List<S3Object> {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    return bucketStore.lookupIdsInBucket(prefix, bucketName)
      .mapNotNull { id -> objectStore.getS3ObjectMetadata(bucketMetadata, id, null) }
      .map(S3Object::from)
      .sortedBy(S3Object::key)
  }

  fun listVersions(
    bucketName: String,
    prefix: String?,
    delimiter: String?,
    encodingType: String?,
    maxKeys: Int,
    keyMarker: String?,
    versionIdMarker: String?
  ): ListVersionsResult {
    val result = listObjectsV1(bucketName, prefix, delimiter, keyMarker, encodingType, maxKeys)

    val bucket = bucketStore.getBucketMetadata(bucketName)
    val objectVersions = arrayListOf<ObjectVersion>()
    val deleteMarkers = arrayListOf<DeleteMarkerEntry>()
    var nextVersionIdMarker: String? = null

    for (content in result.contents) {
      if (nextVersionIdMarker != null) break

      val id = bucket.getID(content.key)
      if (bucket.isVersioningEnabled) {
        val s3ObjectVersions = objectStore.getS3ObjectVersions(bucket, id!!)
        val versions = ArrayList<String?>(s3ObjectVersions.versions).apply { reverse() }

        for (s3ObjectVersion in versions) {
          val meta = objectStore.getS3ObjectMetadata(bucket, id, s3ObjectVersion)!!
          if (!meta.deleteMarker) {
            if (objectVersions.size > maxKeys) {
              nextVersionIdMarker = s3ObjectVersion
              break
            }
            objectVersions += ObjectVersion.from(meta, s3ObjectVersions.latestVersion == s3ObjectVersion)
          } else {
            deleteMarkers += DeleteMarkerEntry.from(meta, s3ObjectVersions.latestVersion == s3ObjectVersion)
          }
        }
      } else {
        objectVersions += ObjectVersion.from(content)
      }
    }

    return ListVersionsResult(
      result.commonPrefixes,
      deleteMarkers,
      delimiter,
      result.encodingType,
      result.isTruncated,
      keyMarker,
      result.maxKeys,
      result.name,
      result.nextMarker,
      nextVersionIdMarker,
      result.prefix,
      objectVersions,
      versionIdMarker
    )
  }

  fun listObjectsV2(
    bucketName: String,
    prefix: String?,
    delimiter: String?,
    encodingType: String?,
    startAfter: String?,
    maxKeys: Int,
    continuationToken: String?,
    fetchOwner: Boolean
  ): ListBucketResultV2 {
    if (maxKeys == 0) {
      return ListBucketResultV2(
        listOf(),
        listOf(),
        continuationToken,
        delimiter,
        encodingType,
        false,
        "0",
        maxKeys,
        bucketName,
        null,
        prefix,
        null
      )
    }

    var contents = getS3Objects(bucketName, prefix)

    if (!fetchOwner) {
      contents = mapContents(contents) {
        S3Object(
          it.checksumAlgorithm,
          it.checksumType,
          it.etag,
          it.key,
          it.lastModified,
          null,
          null,
          it.size,
          it.storageClass
        )
      }
    }

    var nextContinuationToken: String? = null
    var isTruncated = false

    if (continuationToken != null) {
      val continueAfter = listObjectsPagingStateCache.remove(continuationToken)
      contents = filterBy(contents, S3Object::key, continueAfter)
    } else {
      contents = filterBy(contents, S3Object::key, startAfter)
    }

    val commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key)
    contents = filterBy(contents, S3Object::key, commonPrefixes)

    if (contents.size > maxKeys) {
      isTruncated = true
      nextContinuationToken = UUID.randomUUID().toString()
      contents = contents.subList(0, maxKeys)
      listObjectsPagingStateCache[nextContinuationToken] = contents[maxKeys - 1].key
    }

    var returnDelimiter = delimiter
    var returnPrefix = prefix
    var returnStartAfter = startAfter
    var returnCommonPrefixes = commonPrefixes

    if (encodingType == "url") {
      contents = contents.map {
        S3Object(
          it.checksumAlgorithm,
          it.checksumType,
          it.etag,
          urlEncodeIgnoreSlashes(it.key),
          it.lastModified,
          it.owner,
          it.restoreStatus,
          it.size,
          it.storageClass
        )
      }
      returnPrefix = urlEncodeIgnoreSlashes(prefix)
      returnStartAfter = urlEncodeIgnoreSlashes(startAfter)
      returnCommonPrefixes = commonPrefixes.map { urlEncodeIgnoreSlashes(it) }
      returnDelimiter = urlEncodeIgnoreSlashes(delimiter)
    }

    return ListBucketResultV2(
      returnCommonPrefixes.map { Prefix(it) },
      contents,
      continuationToken,
      returnDelimiter,
      encodingType,
      isTruncated,
      contents.size.toString(),
      maxKeys,
      bucketName,
      nextContinuationToken,
      returnPrefix,
      returnStartAfter
    )
  }

  @Deprecated("")
  fun listObjectsV1(
    bucketName: String,
    prefix: String?,
    delimiter: String?,
    marker: String?,
    encodingType: String?,
    maxKeys: Int
  ): ListBucketResult {
    if (maxKeys == 0) {
      return ListBucketResult(
        emptyList(),
        listOf(),
        null,
        encodingType,
        false,
        marker,
        maxKeys,
        bucketName,
        marker,
        prefix
      )
    }

    var contents = getS3Objects(bucketName, prefix)
    contents = filterBy(contents, S3Object::key, marker)

    var isTruncated = false
    var nextMarker: String? = null

    val commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key)
    contents = filterBy(contents, S3Object::key, commonPrefixes)

    if (maxKeys < contents.size) {
      contents = contents.subList(0, maxKeys)
      isTruncated = true
      if (maxKeys > 0) {
        nextMarker = contents[maxKeys - 1].key
      }
    }

    var returnPrefix = prefix
    var returnCommonPrefixes = commonPrefixes

    if (encodingType == "url") {
      contents = contents.map {
        S3Object(
          it.checksumAlgorithm,
          it.checksumType,
          it.etag,
          urlEncodeIgnoreSlashes(it.key),
          it.lastModified,
          it.owner,
          it.restoreStatus,
          it.size,
          it.storageClass
        )
      }
      returnPrefix = urlEncodeIgnoreSlashes(prefix)
      returnCommonPrefixes = commonPrefixes.map { urlEncodeIgnoreSlashes(it) }
    }

    return ListBucketResult(
      returnCommonPrefixes.map { Prefix(it) },
      contents,
      delimiter,
      encodingType,
      isTruncated,
      marker,
      maxKeys,
      bucketName,
      nextMarker,
      returnPrefix
    )
  }

  fun bucketLocationHeaders(bucketMetadata: BucketMetadata): Map<String, String> {
    val info = bucketMetadata.bucketInfo
    val loc = bucketMetadata.locationInfo
    return if (
      info?.type == BucketType.DIRECTORY &&
      loc?.name != null &&
      loc.type != null
    ) {
      mapOf(
        X_AMZ_BUCKET_LOCATION_NAME to loc.name,
        X_AMZ_BUCKET_LOCATION_TYPE to loc.type.toString()
      )
    } else {
      mapOf()
    }
  }

  fun verifyBucketExists(bucketName: String): BucketMetadata {
    return if (!bucketStore.doesBucketExist(bucketName)) {
      throw S3Exception.NO_SUCH_BUCKET
    } else {
      bucketStore.getBucketMetadata(bucketName)
    }
  }

  fun verifyBucketObjectLockEnabled(bucketName: String) {
    if (!bucketStore.isObjectLockEnabled(bucketName)) {
      throw S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK
    }
  }

  /**
   * Validates S3 bucket names according to the documented constraints.
   * [API Reference Bucket Naming](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html).
   */
  fun verifyBucketNameIsAllowed(bucketName: String) {
    if (bucketName.isBlank()) throw S3Exception.INVALID_BUCKET_NAME

    // Allowed chars and length (3..63)
    if (!ALLOWED_CHARS_AND_LENGTH.matches(bucketName)) throw S3Exception.INVALID_BUCKET_NAME

    // Must start and end with a letter or number
    if (!STARTS_AND_ENDS_WITH_ALNUM.matches(bucketName)) throw S3Exception.INVALID_BUCKET_NAME

    // Must not contain two adjacent periods
    if (ADJACENT_DOTS.containsMatchIn(bucketName)) throw S3Exception.INVALID_BUCKET_NAME

    // Must not be formatted as an IP address (e.g., 192.168.5.4)
    if (IP_LIKE_FOUR_PARTS.matches(bucketName) && isValidIpv4(bucketName)) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Disallowed prefixes
    when {
      bucketName.startsWith("xn--") -> throw S3Exception.INVALID_BUCKET_NAME
      bucketName.startsWith("sthree-") -> throw S3Exception.INVALID_BUCKET_NAME
      bucketName.startsWith("amzn-s3-demo-") -> throw S3Exception.INVALID_BUCKET_NAME
    }
  }

  fun verifyBucketIsEmpty(bucketName: String) {
    if (!isBucketEmpty(bucketName)) {
      throw S3Exception.BUCKET_NOT_EMPTY
    }
  }

  fun verifyBucketDoesNotExist(bucketName: String) {
    if (bucketStore.doesBucketExist(bucketName)) {
      // In S3Mock all buckets have the same owner; if it exists, it's owned by you.
      throw S3Exception.BUCKET_ALREADY_OWNED_BY_YOU
    }
  }

  fun verifyMaxKeys(maxKeys: Int) {
    if (maxKeys < 0) throw S3Exception.INVALID_REQUEST_MAX_KEYS
  }

  fun verifyEncodingType(encodingType: String?) {
    if (!encodingType.isNullOrEmpty() && encodingType != "url") {
      throw S3Exception.INVALID_REQUEST_ENCODING_TYPE
    }
  }

  companion object {
    // Validation patterns per S3 bucket naming rules
    private val ALLOWED_CHARS_AND_LENGTH = Regex("^[a-z0-9.-]{3,63}$")
    private val STARTS_AND_ENDS_WITH_ALNUM = Regex("^[a-z0-9].*[a-z0-9]$")
    private val ADJACENT_DOTS = Regex("\\.\\.")
    private val IP_LIKE_FOUR_PARTS = Regex("^(\\d{1,3}\\.){3}\\d{1,3}$")

    // Parses and validates IPv4 octets (0..255), rejects leading zeros.
    private fun isValidIpv4(s: String): Boolean {
      val parts = s.split('.')
      if (parts.size != 4) return false

      for (p in parts) {
        if (p.isEmpty() || p.length > 3) return false
        val value = p.toIntOrNull() ?: return false
        if (value !in 0..255) return false
        if (p.length > 1 && p.startsWith('0')) return false
      }
      return true
    }
  }
}
