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
import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.util.Map
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

open class BucketService(private val bucketStore: BucketStore, private val objectStore: ObjectStore) : ServiceBase() {
  private val listObjectsPagingStateCache: MutableMap<String, String> = ConcurrentHashMap<String, String>()
  private val listBucketsPagingStateCache: MutableMap<String, String> = ConcurrentHashMap<String, String>()

  fun isBucketEmpty(bucketName: String): Boolean {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val objects: MutableMap<String, UUID> = bucketMetadata.objects
    if (!objects.isEmpty()) {
      for (id in objects.values) {
        val s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucketMetadata, id, null)
        if (s3ObjectMetadata != null && !s3ObjectMetadata.deleteMarker) {
          return false
        }
      }
      return true
    }
    return bucketMetadata.objects.isEmpty()
  }

  fun doesBucketExist(bucketName: String): Boolean {
    return bucketStore.doesBucketExist(bucketName)
  }

  fun listBuckets(
    bucketRegion: Region?,
    continuationToken: String?,
    maxBuckets: Int,
    prefix: String?
  ): ListAllMyBucketsResult {
    val normalizedPrefix = prefix.orEmpty()

    var buckets = bucketStore
      .listBuckets()
      .asSequence()
      .filter { it.name.startsWith(normalizedPrefix) }
      .sortedBy { it.name }
      .map { Bucket.from(it) }
      .toList()

    bucketRegion?.let { region ->
      buckets = buckets.filter { it.bucketRegion == region.toString() }
    }

    var nextContinuationToken: String? = null

    continuationToken?.let { token ->
      val continueAfter = listBucketsPagingStateCache.remove(token)
      buckets = filterBy(buckets, Bucket::name, continueAfter)
    }

    if (buckets.size > maxBuckets) {
      nextContinuationToken = UUID.randomUUID().toString()
      buckets = buckets.subList(0, maxBuckets)
      listBucketsPagingStateCache[nextContinuationToken] = buckets[maxBuckets - 1].name
    }

    return ListAllMyBucketsResult(Owner.DEFAULT_OWNER, Buckets(buckets), prefix, nextContinuationToken)

  }

  fun getBucket(bucketName: String): Bucket {
    return Bucket.from(bucketStore.getBucketMetadata(bucketName))
  }

  fun createBucket(
    bucketName: String,
    objectLockEnabled: Boolean,
    objectOwnership: ObjectOwnership,
    bucketRegion: String?,
    bucketInfo: BucketInfo?,
    locationInfo: LocationInfo?
  ): Bucket {
    return Bucket.from(
      bucketStore.createBucket(
        bucketName,
        objectLockEnabled,
        objectOwnership,
        bucketRegion,
        bucketInfo,
        locationInfo
      )
    )
  }

  fun deleteBucket(bucketName: String): Boolean {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val objects: MutableMap<String, UUID> = bucketMetadata.objects
    if (!objects.isEmpty()) {
      for (entry in objects.entries) {
        val s3ObjectMetadata =
          objectStore.getS3ObjectMetadata(bucketMetadata, entry.value, null)
        if (s3ObjectMetadata != null && s3ObjectMetadata.deleteMarker) {
          // yes, we really want to delete the objects here, if they are delete markers, they
          // do not officially exist.
          objectStore.doDeleteObject(bucketMetadata, entry.value)
          bucketStore.removeFromBucket(entry.key, bucketName)
        }
      }
    }
    // check again if bucket is empty
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
    val configuration = bucketMetadata.versioningConfiguration
    if (configuration != null) {
      return configuration
    } else {
      throw S3Exception.NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION
    }
  }

  fun setObjectLockConfiguration(bucketName: String, configuration: ObjectLockConfiguration) {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    bucketStore.storeObjectLockConfiguration(bucketMetadata, configuration)
  }

  fun getObjectLockConfiguration(bucketName: String): ObjectLockConfiguration {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    val objectLockConfiguration = bucketMetadata.objectLockConfiguration
    if (objectLockConfiguration != null) {
      return objectLockConfiguration
    } else {
      throw S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK
    }
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
    val configuration = bucketMetadata.bucketLifecycleConfiguration
    if (configuration != null) {
      return configuration
    } else {
      throw S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION
    }
  }

  fun getS3Objects(bucketName: String, prefix: String?): List<S3Object> {
    val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
    return bucketStore.lookupIdsInBucket(prefix, bucketName)
      .asSequence()
      .mapNotNull { id -> objectStore.getS3ObjectMetadata(bucketMetadata, id, null) }
      .map { meta -> S3Object.from(meta) } // List Objects results are expected to be sorted by key
      .sortedBy(S3Object::key)
      .toList()
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
    val objectVersions = ArrayList<ObjectVersion>()
    val deleteMarkers = ArrayList<DeleteMarkerEntry>()
    var nextVersionIdMarker: String? = null

    for (content in result.contents) {
      if (nextVersionIdMarker != null) {
        break
      }
      val id = bucket.getID(content.key)

      if (bucket.isVersioningEnabled) {
        val s3ObjectVersions = objectStore.getS3ObjectVersions(bucket, id!!)
        val versions = ArrayList<String?>(s3ObjectVersions.versions)
        versions.reverse()
        for (s3ObjectVersion in versions) {
          val s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucket, id, s3ObjectVersion)
          if (!s3ObjectMetadata!!.deleteMarker) {
            if (objectVersions.size > maxKeys) {
              nextVersionIdMarker = s3ObjectVersion
              break
            }
            objectVersions.add(
              ObjectVersion.from(
                s3ObjectMetadata,
                s3ObjectVersions.latestVersion == s3ObjectVersion
              )
            )
          } else {
            deleteMarkers.add(
              DeleteMarkerEntry.from(
                s3ObjectMetadata,
                s3ObjectVersions.latestVersion == s3ObjectVersion
              )
            )
          }
        }
      } else {
        objectVersions.add(ObjectVersion.from(content))
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
        mutableListOf<Prefix>(),
        mutableListOf<S3Object>(),
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
      contents = mapContents(
        contents
      ) {
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
    var nextContinuationToken = null as String?
    var isTruncated = false

    /*
  Start-after is valid only in first request.
  If the response is truncated,
  you can specify this parameter along with the continuation-token parameter,
  and then Amazon S3 ignores this parameter.
 */
    if (continuationToken != null) {
      val continueAfter = listObjectsPagingStateCache[continuationToken]
      contents = filterBy(contents, S3Object::key, continueAfter)
      listObjectsPagingStateCache.remove(continuationToken)
    } else {
      contents = filterBy(contents, S3Object::key, startAfter)
    }

    val commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key)
    contents = filterBy(contents, S3Object::key, commonPrefixes)

    if (contents.size > maxKeys) {
      isTruncated = true
      nextContinuationToken = UUID.randomUUID().toString()
      contents = contents.subList(0, maxKeys)
      listObjectsPagingStateCache[nextContinuationToken] = contents.get(maxKeys - 1).key
    }

    var returnDelimiter = delimiter
    var returnPrefix = prefix
    var returnStartAfter = startAfter
    var returnCommonPrefixes = commonPrefixes

    if ("url" == encodingType) {
      contents = mapContents(
        contents
      ) {
        S3Object(
          it.checksumAlgorithm,
          it.checksumType,
          it.etag,
          SdkHttpUtils.urlEncodeIgnoreSlashes(it.key),
          it.lastModified,
          it.owner,
          it.restoreStatus,
          it.size,
          it.storageClass
        )
      }
      returnPrefix = SdkHttpUtils.urlEncodeIgnoreSlashes(prefix)
      returnStartAfter = SdkHttpUtils.urlEncodeIgnoreSlashes(startAfter)
      returnCommonPrefixes = mapContents(
        commonPrefixes
      ) { value: String? -> SdkHttpUtils.urlEncodeIgnoreSlashes(value) }
      returnDelimiter = SdkHttpUtils.urlEncodeIgnoreSlashes(delimiter)
    }

    return ListBucketResultV2(
      returnCommonPrefixes.stream().map { prefix: String? -> Prefix(prefix) }.toList(),
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
        listOf<Prefix>(), mutableListOf<S3Object>(), null, encodingType,
        false, marker, maxKeys, bucketName, marker, prefix
      )
    }

    var contents = getS3Objects(bucketName, prefix)
    contents = filterBy(contents, S3Object::key, marker)

    var isTruncated = false
    var nextMarker = null as String?

    val commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key)
    contents = filterBy(contents, S3Object::key, commonPrefixes)
    if (maxKeys < contents.size) {
      contents = contents.subList(0, maxKeys)
      isTruncated = true
      if (maxKeys > 0) {
        nextMarker = contents.get(maxKeys - 1).key
      }
    }

    var returnPrefix = prefix
    var returnCommonPrefixes = commonPrefixes

    if ("url" == encodingType) {
      contents = mapContents(
        contents
      ) {
        S3Object(
          it.checksumAlgorithm,
          it.checksumType,
          it.etag,
          SdkHttpUtils.urlEncodeIgnoreSlashes(it.key),
          it.lastModified,
          it.owner,
          it.restoreStatus,
          it.size,
          it.storageClass
        )
      }
      returnPrefix = SdkHttpUtils.urlEncodeIgnoreSlashes(prefix)
      returnCommonPrefixes =
        mapContents(commonPrefixes) { value: String? -> SdkHttpUtils.urlEncodeIgnoreSlashes(value) }
    }

    return ListBucketResult(
      returnCommonPrefixes.stream().map { prefix: String? -> Prefix(prefix) }.toList(),
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

  fun bucketLocationHeaders(bucketMetadata: BucketMetadata): MutableMap<String, String> {
    if (bucketMetadata.bucketInfo != null && bucketMetadata.bucketInfo.type != null && bucketMetadata.bucketInfo.type == BucketType.DIRECTORY && bucketMetadata.locationInfo != null && bucketMetadata.locationInfo.name != null && bucketMetadata.locationInfo.type != null) {
      return Map.of<String, String>(
        X_AMZ_BUCKET_LOCATION_NAME, bucketMetadata.locationInfo.name,
        X_AMZ_BUCKET_LOCATION_TYPE, bucketMetadata.locationInfo.type.toString()
      )
    } else {
      return Map.of<String, String>()
    }
  }

  fun verifyBucketExists(bucketName: String): BucketMetadata {
    if (!bucketStore.doesBucketExist(bucketName)) {
      throw S3Exception.NO_SUCH_BUCKET
    } else {
      return bucketStore.getBucketMetadata(bucketName)
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
    if (bucketName.isBlank()) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Allowed chars and length (3..63)
    if (!ALLOWED_CHARS_AND_LENGTH.matcher(bucketName).matches()) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Must start and end with a letter or number
    if (!STARTS_AND_ENDS_WITH_ALNUM.matcher(bucketName).matches()) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Must not contain two adjacent periods
    if (ADJACENT_DOTS.matcher(bucketName).find()) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Must not be formatted as an IP address (e.g., 192.168.5.4)
    if (IP_LIKE_FOUR_PARTS.matcher(bucketName).matches() && isValidIpv4(bucketName)) {
      throw S3Exception.INVALID_BUCKET_NAME
    }

    // Disallowed prefixes
    if (bucketName.startsWith("xn--")) {
      throw S3Exception.INVALID_BUCKET_NAME
    }
    if (bucketName.startsWith("sthree-")) {
      throw S3Exception.INVALID_BUCKET_NAME
    }
    if (bucketName.startsWith("amzn-s3-demo-")) {
      throw S3Exception.INVALID_BUCKET_NAME
    }
  }

  fun verifyBucketIsEmpty(bucketName: String) {
    if (!isBucketEmpty(bucketName)) {
      throw S3Exception.BUCKET_NOT_EMPTY
    }
  }

  fun verifyBucketDoesNotExist(bucketName: String) {
    if (bucketStore.doesBucketExist(bucketName)) {
      // currently, all buckets have the same owner in S3Mock. If the bucket exists, it's owned by
      // the owner that tries to create the bucket owns the existing bucket too.
      throw S3Exception.BUCKET_ALREADY_OWNED_BY_YOU
    }
  }

  fun verifyMaxKeys(maxKeys: Int) {
    if (maxKeys < 0) {
      throw S3Exception.INVALID_REQUEST_MAX_KEYS
    }
  }

  fun verifyEncodingType(encodingType: String?) {
    if (!encodingType.isNullOrEmpty() && "url" != encodingType) {
      throw S3Exception.INVALID_REQUEST_ENCODING_TYPE
    }
  }

  companion object {
    // Validation patterns per S3 bucket naming rules
    private val ALLOWED_CHARS_AND_LENGTH: Pattern = Pattern.compile("^[a-z0-9.-]{3,63}$")
    private val STARTS_AND_ENDS_WITH_ALNUM: Pattern = Pattern.compile("^[a-z0-9].*[a-z0-9]$")
    private val ADJACENT_DOTS: Pattern = Pattern.compile("\\.\\.")
    private val IP_LIKE_FOUR_PARTS: Pattern = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}$")

    // Parses and validates IPv4 octets (0..255) to avoid false positives like 999.999.999.999
    private fun isValidIpv4(s: String): Boolean {
      val parts = s.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
      if (parts.size != 4) {
        return false
      }
      for (p in parts) {
        if (p.isEmpty() || p.length > 3) {
          return false
        }
        // Disallow leading plus/minus; Pattern already ensures digits only
        val `val`: Int
        try {
          `val` = p.toInt()
        } catch (_: NumberFormatException) {
          return false
        }
        if (`val` !in 0..255) {
          return false
        }
        // Reject octets with leading zeros (e.g., "01") to avoid ambiguity with octal notation.
        // While S3 only cares about the IP format, leading zeros are not allowed in IPv4 addresses
        // per RFC 1123 and RFC 3986, and some systems interpret them as octal. If you need to allow
        // octets with leading zeros (e.g., "01"), you may remove this check, but be aware of potential
        // compatibility and ambiguity issues.
        if (p.length > 1 && p.startsWith("0")) {
          return false
        }
      }
      return true
    }
  }
}
