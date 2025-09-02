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

package com.adobe.testing.s3mock.service;

import static com.adobe.testing.s3mock.S3Exception.BUCKET_ALREADY_OWNED_BY_YOU;
import static com.adobe.testing.s3mock.S3Exception.BUCKET_NOT_EMPTY;
import static com.adobe.testing.s3mock.S3Exception.INVALID_BUCKET_NAME;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_ENCODING_TYPE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_MAX_KEYS;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_BUCKET;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER;
import static com.adobe.testing.s3mock.service.ServiceBase.collapseCommonPrefixes;
import static com.adobe.testing.s3mock.service.ServiceBase.filterBy;
import static com.adobe.testing.s3mock.service.ServiceBase.mapContents;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_NAME;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_LOCATION_TYPE;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.BucketInfo;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.BucketType;
import com.adobe.testing.s3mock.dto.Buckets;
import com.adobe.testing.s3mock.dto.DeleteMarkerEntry;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListVersionsResult;
import com.adobe.testing.s3mock.dto.LocationInfo;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.ObjectOwnership;
import com.adobe.testing.s3mock.dto.ObjectVersion;
import com.adobe.testing.s3mock.dto.Prefix;
import com.adobe.testing.s3mock.dto.Region;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.VersioningConfiguration;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

public class BucketService {
  private final Map<String, String> listObjectsPagingStateCache = new ConcurrentHashMap<>();
  private final Map<String, String> listBucketsPagingStateCache = new ConcurrentHashMap<>();
  private final BucketStore bucketStore;
  private final ObjectStore objectStore;

  public BucketService(BucketStore bucketStore, ObjectStore objectStore) {
    this.bucketStore = bucketStore;
    this.objectStore = objectStore;
  }

  public boolean isBucketEmpty(String bucketName) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    if (bucketMetadata != null) {
      var objects = bucketMetadata.objects();
      if (!objects.isEmpty()) {
        for (var id : objects.values()) {
          var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucketMetadata, id, null);
          if (s3ObjectMetadata != null && !s3ObjectMetadata.deleteMarker()) {
            return false;
          }
        }
        return true;
      }
      return bucketMetadata.objects().isEmpty();
    } else {
      throw new IllegalStateException("Requested Bucket does not exist: " + bucketName);
    }
  }

  public boolean doesBucketExist(String bucketName) {
    return bucketStore.doesBucketExist(bucketName);
  }

  public ListAllMyBucketsResult listBuckets(
      @Nullable Region bucketRegion,
      @Nullable String continuationToken,
      Integer maxBuckets,
      @Nullable String prefix) {
    String nextContinuationToken = null;
    var normalizedPrefix = prefix == null ? "" : prefix;

    var buckets = bucketStore
        .listBuckets()
        .stream()
        .filter(Objects::nonNull)
        .filter(b -> b.name().startsWith(normalizedPrefix))
        .sorted(Comparator.comparing(BucketMetadata::name))
        .map(Bucket::from)
        .toList();

    if (bucketRegion != null) {
      buckets = buckets
          .stream()
          .filter(b -> b.bucketRegion().equals(bucketRegion.toString()))
          .toList();
    }

    if (continuationToken != null) {
      var continueAfter = listBucketsPagingStateCache.get(continuationToken);
      buckets = filterBy(buckets, Bucket::name, continueAfter);
      listBucketsPagingStateCache.remove(continuationToken);
    }

    if (buckets.size() > maxBuckets) {
      nextContinuationToken = UUID.randomUUID().toString();
      buckets = buckets.subList(0, maxBuckets);
      listBucketsPagingStateCache.put(nextContinuationToken, buckets.get(maxBuckets - 1).name());
    }

    return new ListAllMyBucketsResult(DEFAULT_OWNER, new Buckets(buckets), prefix, nextContinuationToken);
  }

  public Bucket getBucket(String bucketName) {
    return Bucket.from(bucketStore.getBucketMetadata(bucketName));
  }

  public Bucket createBucket(
      String bucketName,
      boolean objectLockEnabled,
      ObjectOwnership objectOwnership,
      @Nullable String bucketRegion,
      @Nullable BucketInfo bucketInfo,
      @Nullable LocationInfo locationInfo) {
    return Bucket.from(
        bucketStore.createBucket(bucketName,
            objectLockEnabled,
            objectOwnership,
            bucketRegion,
            bucketInfo,
            locationInfo
        )
    );
  }

  public boolean deleteBucket(String bucketName) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    if (bucketMetadata != null) {
      var objects = bucketMetadata.objects();
      if (!objects.isEmpty()) {
        for (var entry : objects.entrySet()) {
          var s3ObjectMetadata =
              objectStore.getS3ObjectMetadata(bucketMetadata, entry.getValue(), null);
          if (s3ObjectMetadata != null && s3ObjectMetadata.deleteMarker()) {
            //yes, we really want to delete the objects here, if they are delete markers, they
            //do not officially exist.
            objectStore.doDeleteObject(bucketMetadata, entry.getValue());
            bucketStore.removeFromBucket(entry.getKey(), bucketName);
          }
        }
      }
      //check again if bucket is empty
      bucketMetadata = bucketStore.getBucketMetadata(bucketName);
      if (!bucketMetadata.objects().isEmpty()) {
        throw new IllegalStateException("Bucket is not empty: " + bucketName);
      }
      return bucketStore.deleteBucket(bucketName);
    } else {
      throw new IllegalStateException("Requested Bucket does not exist: " + bucketName);
    }
  }

  public void setVersioningConfiguration(String bucketName, VersioningConfiguration configuration) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    bucketStore.storeVersioningConfiguration(bucketMetadata, configuration);
  }

  public VersioningConfiguration getVersioningConfiguration(String bucketName) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var configuration = bucketMetadata.versioningConfiguration();
    if (configuration != null) {
      return configuration;
    } else {
      throw NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION;
    }
  }

  public void setObjectLockConfiguration(String bucketName, ObjectLockConfiguration configuration) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    bucketStore.storeObjectLockConfiguration(bucketMetadata, configuration);
  }

  public ObjectLockConfiguration getObjectLockConfiguration(String bucketName) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var objectLockConfiguration = bucketMetadata.objectLockConfiguration();
    if (objectLockConfiguration != null) {
      return objectLockConfiguration;
    } else {
      throw NOT_FOUND_BUCKET_OBJECT_LOCK;
    }
  }

  public void setBucketLifecycleConfiguration(
      String bucketName,
      @Nullable BucketLifecycleConfiguration configuration) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    bucketStore.storeBucketLifecycleConfiguration(bucketMetadata, configuration);
  }

  public void deleteBucketLifecycleConfiguration(String bucketName) {
    setBucketLifecycleConfiguration(bucketName, null);
  }

  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var configuration = bucketMetadata.bucketLifecycleConfiguration();
    if (configuration != null) {
      return configuration;
    } else {
      throw NO_SUCH_LIFECYCLE_CONFIGURATION;
    }
  }

  public List<S3Object> getS3Objects(String bucketName, @Nullable String prefix) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuids = bucketStore.lookupIdsInBucket(prefix, bucketName);
    return uuids
        .stream()
        .filter(Objects::nonNull)
        .map(uuid -> objectStore.getS3ObjectMetadata(bucketMetadata, uuid, null))
        .filter(Objects::nonNull)
        .map(S3Object::from)
        // List Objects results are expected to be sorted by key
        .sorted(Comparator.comparing(S3Object::key))
        .toList();
  }

  public ListVersionsResult listVersions(
      String bucketName,
      @Nullable String prefix,
      @Nullable String delimiter,
      @Nullable String encodingType,
      Integer maxKeys,
      @Nullable String keyMarker,
      @Nullable String versionIdMarker
  ) {
    var result = listObjectsV1(bucketName, prefix, delimiter, keyMarker, encodingType, maxKeys);

    var bucket = bucketStore.getBucketMetadata(bucketName);
    var objectVersions = new ArrayList<ObjectVersion>();
    var deleteMarkers = new ArrayList<DeleteMarkerEntry>();
    String nextVersionIdMarker = null;

    for (var object : result.contents()) {
      if (nextVersionIdMarker != null) {
        break;
      }
      var id = bucket.getID(object.key());

      if (bucket.isVersioningEnabled()) {
        var s3ObjectVersions = objectStore.getS3ObjectVersions(bucket, id);
        var versions = new ArrayList<>(s3ObjectVersions.versions());
        Collections.reverse(versions);
        for (var s3ObjectVersion : versions) {
          var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucket, id, s3ObjectVersion);
          if (!s3ObjectMetadata.deleteMarker()) {
            if (objectVersions.size() > maxKeys) {
              nextVersionIdMarker = s3ObjectVersion;
              break;
            }
            objectVersions.add(
                ObjectVersion.from(s3ObjectMetadata,
                    Objects.equals(s3ObjectVersions.getLatestVersion(), s3ObjectVersion))
            );
          } else {
            deleteMarkers.add(
                DeleteMarkerEntry.from(s3ObjectMetadata,
                    Objects.equals(s3ObjectVersions.getLatestVersion(), s3ObjectVersion)));
          }
        }
      } else {
        objectVersions.add(ObjectVersion.from(object));
      }
    }

    return new ListVersionsResult(result.commonPrefixes(), deleteMarkers, delimiter, result.encodingType(),
        result.isTruncated(), keyMarker, result.maxKeys(), result.name(),
        result.nextMarker(), nextVersionIdMarker, result.prefix(),
        objectVersions, versionIdMarker
    );
  }

  public ListBucketResultV2 listObjectsV2(
      String bucketName,
      @Nullable String prefix,
      @Nullable String delimiter,
      @Nullable String encodingType,
      @Nullable String startAfter,
      Integer maxKeys,
      @Nullable String continuationToken,
      boolean fetchOwner) {

    if (maxKeys == 0) {
      return new ListBucketResultV2(List.of(), List.of(), continuationToken,
          delimiter, encodingType, false, maxKeys, bucketName, null, prefix,
          "0", null);
    }

    var contents = getS3Objects(bucketName, prefix);
    if (!fetchOwner) {
      contents = mapContents(contents,
          object -> new S3Object(object.checksumAlgorithm(),
              object.checksumType(),
              object.etag(),
              object.key(),
              object.lastModified(),
              null,
              null,
              object.size(),
              object.storageClass()
          )
      );
    }
    var nextContinuationToken = (String) null;
    var isTruncated = false;

    /*
      Start-after is valid only in first request.
      If the response is truncated,
      you can specify this parameter along with the continuation-token parameter,
      and then Amazon S3 ignores this parameter.
     */
    if (continuationToken != null) {
      var continueAfter = listObjectsPagingStateCache.get(continuationToken);
      contents = filterBy(contents, S3Object::key, continueAfter);
      listObjectsPagingStateCache.remove(continuationToken);
    } else {
      contents = filterBy(contents, S3Object::key, startAfter);
    }

    var commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key);
    contents = filterBy(contents, S3Object::key, commonPrefixes);

    if (contents.size() > maxKeys) {
      isTruncated = true;
      nextContinuationToken = UUID.randomUUID().toString();
      contents = contents.subList(0, maxKeys);
      listObjectsPagingStateCache.put(nextContinuationToken,
          contents.get(maxKeys - 1).key());
    }

    var returnDelimiter = delimiter;
    var returnPrefix = prefix;
    var returnStartAfter = startAfter;
    var returnCommonPrefixes = commonPrefixes;

    if (Objects.equals("url", encodingType)) {
      contents = mapContents(contents,
          object -> new S3Object(object.checksumAlgorithm(),
              object.checksumType(),
              object.etag(),
              urlEncodeIgnoreSlashes(object.key()),
              object.lastModified(),
              object.owner(), object.restoreStatus(), object.size(),
              object.storageClass()
          ));
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnStartAfter = urlEncodeIgnoreSlashes(startAfter);
      returnCommonPrefixes = mapContents(commonPrefixes, SdkHttpUtils::urlEncodeIgnoreSlashes);
      returnDelimiter = urlEncodeIgnoreSlashes(delimiter);
    }

    return new ListBucketResultV2(
        returnCommonPrefixes.stream().map(Prefix::new).toList(),
        contents,
        continuationToken,
        returnDelimiter,
        encodingType,
        isTruncated,
        maxKeys,
        bucketName,
        nextContinuationToken,
        returnPrefix,
        String.valueOf(contents.size()),
        returnStartAfter
    );
  }

  @Deprecated(since = "2.12.2", forRemoval = true)
  public ListBucketResult listObjectsV1(
      String bucketName,
      @Nullable String prefix,
      @Nullable String delimiter,
      @Nullable String marker,
      @Nullable String encodingType,
      Integer maxKeys) {

    if (maxKeys == 0) {
      return new ListBucketResult(List.of(), List.of(), null,  encodingType,
          false, marker, maxKeys, bucketName, marker, prefix);
    }

    var contents = getS3Objects(bucketName, prefix);
    contents = filterBy(contents, S3Object::key, marker);

    var isTruncated = false;
    var nextMarker = (String) null;

    var commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, S3Object::key);
    contents = filterBy(contents, S3Object::key, commonPrefixes);
    if (maxKeys < contents.size()) {
      contents = contents.subList(0, maxKeys);
      isTruncated = true;
      if (maxKeys > 0) {
        nextMarker = contents.get(maxKeys - 1).key();
      }
    }

    var returnPrefix = prefix;
    var returnCommonPrefixes = commonPrefixes;

    if (Objects.equals("url", encodingType)) {
      contents = mapContents(contents,
          object -> new S3Object(object.checksumAlgorithm(),
              object.checksumType(),
              object.etag(),
              urlEncodeIgnoreSlashes(object.key()),
              object.lastModified(),
              object.owner(),
              object.restoreStatus(),
              object.size(),
              object.storageClass()
          ));
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnCommonPrefixes = mapContents(commonPrefixes, SdkHttpUtils::urlEncodeIgnoreSlashes);
    }

    return new ListBucketResult(
        returnCommonPrefixes.stream().map(Prefix::new).toList(),
        contents,
        delimiter,
        encodingType,
        isTruncated,
        marker,
        maxKeys,
        bucketName,
        nextMarker,
        returnPrefix
    );
  }

  public Map<String, String> bucketLocationHeaders(BucketMetadata bucketMetadata) {
    if (bucketMetadata.bucketInfo() != null
        && bucketMetadata.bucketInfo().type() != null
        && bucketMetadata.bucketInfo().type() == BucketType.DIRECTORY
        && bucketMetadata.locationInfo() != null
        && bucketMetadata.locationInfo().name() != null
        && bucketMetadata.locationInfo().type() != null) {
      return Map.of(
          X_AMZ_BUCKET_LOCATION_NAME, bucketMetadata.locationInfo().name(),
          X_AMZ_BUCKET_LOCATION_TYPE, bucketMetadata.locationInfo().type().toString()
      );
    } else {
      return Map.of();
    }
  }

  public BucketMetadata verifyBucketExists(String bucketName) {
    if (!bucketStore.doesBucketExist(bucketName)) {
      throw NO_SUCH_BUCKET;
    } else {
      return bucketStore.getBucketMetadata(bucketName);
    }
  }

  public void verifyBucketObjectLockEnabled(String bucketName) {
    if (!bucketStore.isObjectLockEnabled(bucketName)) {
      throw NOT_FOUND_BUCKET_OBJECT_LOCK;
    }
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">API Reference Bucket Naming</a>.
   */
  public void verifyBucketNameIsAllowed(String bucketName) {
    if (!bucketName.matches("[a-z0-9.-]+")) {
      throw INVALID_BUCKET_NAME;
    }
  }

  public void verifyBucketIsEmpty(String bucketName) {
    if (!isBucketEmpty(bucketName)) {
      throw BUCKET_NOT_EMPTY;
    }
  }

  public void verifyBucketDoesNotExist(String bucketName) {
    if (bucketStore.doesBucketExist(bucketName)) {
      //currently, all buckets have the same owner in S3Mock. If the bucket exists, it's owned by
      //the owner that tries to create the bucket owns the existing bucket too.
      throw BUCKET_ALREADY_OWNED_BY_YOU;
    }
  }

  public void verifyMaxKeys(Integer maxKeys) {
    if (maxKeys < 0) {
      throw INVALID_REQUEST_MAX_KEYS;
    }
  }

  public void verifyEncodingType(String encodingType) {
    if (isNotEmpty(encodingType) && !"url".equals(encodingType)) {
      throw INVALID_REQUEST_ENCODING_TYPE;
    }
  }
}
