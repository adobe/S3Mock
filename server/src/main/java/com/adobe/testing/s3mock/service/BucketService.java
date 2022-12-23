/*
 *  Copyright 2017-2022 Adobe.
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

import static com.adobe.testing.s3mock.S3Exception.BUCKET_ALREADY_EXISTS;
import static com.adobe.testing.s3mock.S3Exception.BUCKET_ALREADY_OWNED_BY_YOU;
import static com.adobe.testing.s3mock.S3Exception.BUCKET_NOT_EMPTY;
import static com.adobe.testing.s3mock.S3Exception.INVALID_BUCKET_NAME;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_ENCODINGTYPE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_MAXKEYS;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_BUCKET_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_BUCKET;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

public class BucketService {
  private final Map<String, String> listObjectsPagingStateCache = new ConcurrentHashMap<>();
  private final BucketStore bucketStore;
  private final ObjectStore objectStore;

  public BucketService(BucketStore bucketStore, ObjectStore objectStore) {
    this.bucketStore = bucketStore;
    this.objectStore = objectStore;
  }

  public boolean isBucketEmpty(String bucketName) {
    return bucketStore.isBucketEmpty(bucketName);
  }

  public boolean doesBucketExist(String bucketName) {
    return bucketStore.doesBucketExist(bucketName);
  }

  public ListAllMyBucketsResult listBuckets() {
    List<Bucket> buckets = bucketStore
        .listBuckets()
        .stream()
        .filter(Objects::nonNull)
        .map(Bucket::from)
        .collect(Collectors.toList());
    return new ListAllMyBucketsResult(DEFAULT_OWNER, buckets);
  }

  /**
   * Retrieves a Bucket identified by its name.
   *
   * @param bucketName of the Bucket to be retrieved
   *
   * @return the Bucket or null if not found
   */
  public Bucket getBucket(String bucketName) {
    return Bucket.from(bucketStore.getBucketMetadata(bucketName));
  }

  /**
   * Creates a Bucket identified by its name.
   *
   * @param bucketName of the Bucket to be created
   *
   * @return the Bucket
   */
  public Bucket createBucket(String bucketName, boolean objectLockEnabled) {
    return Bucket.from(bucketStore.createBucket(bucketName, objectLockEnabled));
  }

  public boolean deleteBucket(String bucketName) {
    return bucketStore.deleteBucket(bucketName);
  }

  public void setObjectLockConfiguration(String bucketName, ObjectLockConfiguration configuration) {
    bucketStore.storeObjectLockConfiguration(bucketName, configuration);
  }

  public ObjectLockConfiguration getObjectLockConfiguration(String bucketName) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    ObjectLockConfiguration objectLockConfiguration = bucketMetadata.getObjectLockConfiguration();
    if (objectLockConfiguration != null) {
      return objectLockConfiguration;
    } else {
      throw NOT_FOUND_BUCKET_OBJECT_LOCK;
    }
  }

  public void setBucketLifecycleConfiguration(String bucketName,
      BucketLifecycleConfiguration configuration) {
    bucketStore.storeBucketLifecycleConfiguration(bucketName, configuration);
  }

  public void deleteBucketLifecycleConfiguration(String bucketName) {
    setBucketLifecycleConfiguration(bucketName, null);
  }

  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    BucketLifecycleConfiguration configuration = bucketMetadata.getBucketLifecycleConfiguration();
    if (configuration != null) {
      return configuration;
    } else {
      throw NO_SUCH_LIFECYCLE_CONFIGURATION;
    }
  }

  /**
   * Retrieves S3Objects from a bucket.
   *
   * @param bucketName the Bucket in which to list the file(s) in.
   * @param prefix {@link String} object file name starts with
   *
   * @return S3Objects found in bucket for the given prefix
   */
  public List<S3Object> getS3Objects(String bucketName, String prefix) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    List<UUID> uuids = bucketStore.lookupKeysInBucket(prefix, bucketName);
    return uuids
        .stream()
        .map(uuid -> objectStore.getS3ObjectMetadata(bucketMetadata, uuid))
        .filter(Objects::nonNull)
        .map(S3Object::from)
        // List Objects results are expected to be sorted by key
        .sorted(Comparator.comparing(S3Object::getKey))
        .collect(Collectors.toList());
  }

  public ListBucketResultV2 listObjectsV2(String bucketName,
      String prefix,
      String delimiter,
      String encodingType,
      String startAfter,
      Integer maxKeys,
      String continuationToken) {

    List<S3Object> contents = getS3Objects(bucketName, prefix);
    String nextContinuationToken = null;
    boolean isTruncated = false;

    /*
      Start-after is valid only in first request.
      If the response is truncated,
      you can specify this parameter along with the continuation-token parameter,
      and then Amazon S3 ignores this parameter.
     */
    if (continuationToken != null) {
      String continueAfter = listObjectsPagingStateCache.get(continuationToken);
      contents = filterObjectsBy(contents, continueAfter);
      listObjectsPagingStateCache.remove(continuationToken);
    } else {
      contents = filterObjectsBy(contents, startAfter);
    }

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
    contents = filterObjectsBy(contents, commonPrefixes);

    if (contents.size() > maxKeys) {
      isTruncated = true;
      nextContinuationToken = UUID.randomUUID().toString();
      contents = contents.subList(0, maxKeys);
      listObjectsPagingStateCache.put(nextContinuationToken,
          contents.get(maxKeys - 1).getKey());
    }

    String returnPrefix = prefix;
    String returnStartAfter = startAfter;
    List<String> returnCommonPrefixes = commonPrefixes;

    if (Objects.equals("url", encodingType)) {
      contents = apply(contents, (object) -> {
        object.setKey(urlEncodeIgnoreSlashes(object.getKey()));
        return object;
      });
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnStartAfter = urlEncodeIgnoreSlashes(startAfter);
      returnCommonPrefixes = apply(commonPrefixes, SdkHttpUtils::urlEncodeIgnoreSlashes);
    }

    return new ListBucketResultV2(bucketName, returnPrefix, maxKeys,
        isTruncated, contents, returnCommonPrefixes,
        continuationToken, String.valueOf(contents.size()),
        nextContinuationToken, returnStartAfter, encodingType);
  }

  @Deprecated //forRemoval = true
  public ListBucketResult listObjectsV1(String bucketName, String prefix, String delimiter,
      String marker, String encodingType, Integer maxKeys) {

    verifyMaxKeys(maxKeys);
    verifyEncodingType(encodingType);

    List<S3Object> contents = getS3Objects(bucketName, prefix);
    contents = filterObjectsBy(contents, marker);

    boolean isTruncated = false;
    String nextMarker = null;

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
    contents = filterObjectsBy(contents, commonPrefixes);
    if (maxKeys < contents.size()) {
      contents = contents.subList(0, maxKeys);
      isTruncated = true;
      if (maxKeys > 0) {
        nextMarker = contents.get(maxKeys - 1).getKey();
      }
    }

    String returnPrefix = prefix;
    List<String> returnCommonPrefixes = commonPrefixes;

    if (Objects.equals("url", encodingType)) {
      contents = apply(contents, (object) -> {
        object.setKey(urlEncodeIgnoreSlashes(object.getKey()));
        return object;
      });
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnCommonPrefixes = apply(commonPrefixes, SdkHttpUtils::urlEncodeIgnoreSlashes);
    }

    return new ListBucketResult(bucketName, returnPrefix, marker, maxKeys, isTruncated,
        encodingType, nextMarker, contents, returnCommonPrefixes);
  }

  public void verifyBucketExists(String bucketName) {
    if (!bucketStore.doesBucketExist(bucketName)) {
      throw NO_SUCH_BUCKET;
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
    if (!bucketStore.isBucketEmpty(bucketName)) {
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
      throw INVALID_REQUEST_MAXKEYS;
    }
  }

  public void verifyEncodingType(String encodingType) {
    if (isNotEmpty(encodingType) && !"url".equals(encodingType)) {
      throw INVALID_REQUEST_ENCODINGTYPE;
    }
  }


  /**
   * Collapse all bucket elements with keys starting with some prefix up to the given delimiter into
   * one prefix entry. Collapsed elements are removed from the contents list.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
   *
   * @param queryPrefix the key prefix as specified in the list request
   * @param delimiter the delimiter used to separate a prefix from the rest of the object name
   * @param s3Objects the list of objects to use for collapsing the prefixes
   */
  static List<String> collapseCommonPrefixes(String queryPrefix, String delimiter,
      List<S3Object> s3Objects) {
    List<String> commonPrefixes = new ArrayList<>();
    if (isEmpty(delimiter)) {
      return commonPrefixes;
    }

    String normalizedQueryPrefix = queryPrefix == null ? "" : queryPrefix;

    for (S3Object c : s3Objects) {
      String key = c.getKey();
      if (key.startsWith(normalizedQueryPrefix)) {
        int delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length());
        if (delimiterIndex > 0) {
          String commonPrefix = key.substring(0, delimiterIndex + delimiter.length());
          if (!commonPrefixes.contains(commonPrefix)) {
            commonPrefixes.add(commonPrefix);
          }
        }
      }
    }
    return commonPrefixes;
  }

  private static <T> List<T> apply(List<T> contents, Function<T, T> extractor) {
    return contents
        .stream()
        .map(extractor)
        .collect(Collectors.toList());
  }

  static List<S3Object> filterObjectsBy(List<S3Object> s3Objects,
      String startAfter) {
    if (isNotEmpty(startAfter)) {
      return s3Objects
          .stream()
          .filter(p -> p.getKey().compareTo(startAfter) > 0)
          .collect(Collectors.toList());
    } else {
      return s3Objects;
    }
  }

  static List<S3Object> filterObjectsBy(List<S3Object> s3Objects,
      List<String> commonPrefixes) {
    if (commonPrefixes != null && !commonPrefixes.isEmpty()) {
      return s3Objects
          .stream()
          .filter(c -> commonPrefixes
              .stream()
              .noneMatch(p -> c.getKey().startsWith(p))
          )
          .collect(Collectors.toList());
    } else {
      return s3Objects;
    }
  }
}
