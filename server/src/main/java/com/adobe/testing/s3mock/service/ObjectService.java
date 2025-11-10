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

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_CONTENT;
import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_MD5;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_RETAIN_DATE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_TAG;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NOT_MODIFIED;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY_DELETE_MARKER;
import static com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.DeletedS3Object;
import com.adobe.testing.s3mock.dto.Error;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectService extends ServiceBase {
  static final String WILDCARD_ETAG = "\"*\"";
  static final String WILDCARD = "*";
  private static final Logger LOG = LoggerFactory.getLogger(ObjectService.class);
  private static final Pattern TAG_ALLOWED_CHARS = Pattern.compile("[\\w+ \\-=.:/@]*");
  private static final int MAX_ALLOWED_TAGS = 50;
  private static final int MIN_ALLOWED_TAG_KEY_LENGTH = 1;
  private static final int MAX_ALLOWED_TAG_KEY_LENGTH = 128;
  private static final int MIN_ALLOWED_TAG_VALUE_LENGTH = 0;
  private static final int MAX_ALLOWED_TAG_VALUE_LENGTH = 256;
  private static final String DISALLOWED_TAG_KEY_PREFIX = "aws:";

  private final BucketStore bucketStore;
  private final ObjectStore objectStore;

  public ObjectService(BucketStore bucketStore, ObjectStore objectStore) {
    this.bucketStore = bucketStore;
    this.objectStore = objectStore;
  }

  @Nullable
  public S3ObjectMetadata copyS3Object(
      String sourceBucketName,
      String sourceKey,
      @Nullable String versionId,
      String destinationBucketName,
      String destinationKey,
      Map<String, String> encryptionHeaders,
      Map<String, String> storeHeaders,
      Map<String, String> userMetadata,
      @Nullable StorageClass storageClass) {
    var sourceBucketMetadata = bucketStore.getBucketMetadata(sourceBucketName);
    var destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucketName);
    var sourceId = sourceBucketMetadata.getID(sourceKey);
    if (sourceId == null) {
      return null;
    }

    // source and destination is the same, pretend we copied - S3 does the same.
    if (sourceKey.equals(destinationKey) && sourceBucketName.equals(destinationBucketName)) {
      return objectStore.pretendToCopyS3Object(sourceBucketMetadata,
          sourceId,
          versionId,
          encryptionHeaders,
          storeHeaders,
          userMetadata,
          storageClass);
    }

    // source must be copied to destination
    var destinationId = bucketStore.addKeyToBucket(destinationKey, destinationBucketName);
    try {
      return objectStore.copyS3Object(sourceBucketMetadata, sourceId, versionId,
          destinationBucketMetadata, destinationId, destinationKey,
          encryptionHeaders, storeHeaders, userMetadata, storageClass);
    } catch (Exception e) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucketName);
      throw e;
    }
  }

  public S3ObjectMetadata putS3Object(
      String bucketName,
      String key,
      String contentType,
      Map<String, String> storeHeaders,
      Path path,
      Map<String, String> userMetadata,
      Map<String, String> encryptionHeaders,
      @Nullable List<Tag> tags,
      @Nullable ChecksumAlgorithm checksumAlgorithm,
      @Nullable String checksum,
      Owner owner,
      @Nullable StorageClass storageClass) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      id = bucketStore.addKeyToBucket(key, bucketName);
    }
    return objectStore.storeS3ObjectMetadata(bucketMetadata, id, key, contentType, storeHeaders,
        path, userMetadata, encryptionHeaders, null, tags,
        checksumAlgorithm, checksum, owner, storageClass, ChecksumType.FULL_OBJECT);
  }

  public DeleteResult deleteObjects(String bucketName, Delete delete) {
    var response = new DeleteResult(new ArrayList<>(), new ArrayList<>());
    for (var object : delete.objectsToDelete()) {
      try {
        // ignore result of delete object.
        deleteObject(bucketName, object.key(), object.versionId());
        // add deleted object even if it does not exist S3 does the same.
        if (!delete.quiet()) {
          response.addDeletedObject(DeletedS3Object.from(object));
        }
      } catch (IllegalStateException e) {
        response.addError(
            new Error("InternalError",
                object.key(),
                "We encountered an internal error. Please try again.",
                object.versionId()));
        LOG.error("Object could not be deleted!", e);
      }
    }
    return response;
  }

  public boolean deleteObject(String bucketName, String key, @Nullable String versionId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return false;
    }

    if (objectStore.deleteObject(bucketMetadata, id, versionId)) {
      return bucketStore.removeFromBucket(key, bucketName);
    } else {
      return false;
    }
  }

  /**
   * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions">API Reference</a>.
   */
  public void setObjectTags(String bucketName, String key, @Nullable String versionId, @Nullable List<Tag> tags) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeObjectTags(bucketMetadata, uuid, versionId, tags);
  }

  public void verifyObjectTags(List<Tag> tags) {
    if (tags.size() > MAX_ALLOWED_TAGS) {
      throw INVALID_TAG;
    }
    verifyDuplicateTagKeys(tags);
    for (var tag : tags) {
      verifyTagKeyPrefix(tag.key());
      verifyTagLength(MIN_ALLOWED_TAG_KEY_LENGTH, MAX_ALLOWED_TAG_KEY_LENGTH, tag.key());
      verifyTagChars(tag.key());

      verifyTagLength(MIN_ALLOWED_TAG_VALUE_LENGTH, MAX_ALLOWED_TAG_VALUE_LENGTH, tag.value());
      verifyTagChars(tag.value());
    }
  }

  private void verifyDuplicateTagKeys(List<Tag> tags) {
    var tagKeys = new HashSet<String>();
    for (var tag : tags) {
      if (!tagKeys.add(tag.key())) {
        throw INVALID_TAG;
      }
    }
  }

  private void verifyTagKeyPrefix(String tagKey) {
    if (tagKey.startsWith(DISALLOWED_TAG_KEY_PREFIX)) {
      throw INVALID_TAG;
    }
  }

  private void verifyTagLength(int minLength, int maxLength, String tag) {
    if (tag.length() < minLength || tag.length() > maxLength) {
      throw INVALID_TAG;
    }
  }

  private void verifyTagChars(String tag) {
    if (!TAG_ALLOWED_CHARS.matcher(tag).matches()) {
      throw INVALID_TAG;
    }
  }

  public void setLegalHold(String bucketName, String key, @Nullable String versionId, LegalHold legalHold) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeLegalHold(bucketMetadata, uuid, versionId, legalHold);
  }

  public void setAcl(String bucketName, String key, @Nullable String versionId, AccessControlPolicy policy) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeAcl(bucketMetadata, uuid, versionId, policy);
  }

  public AccessControlPolicy getAcl(String bucketName, String key, @Nullable String versionId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    return objectStore.readAcl(bucketMetadata, uuid, versionId);
  }

  public void setRetention(String bucketName, String key, @Nullable String versionId, Retention retention) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeRetention(bucketMetadata, uuid, versionId, retention);
  }

  public void verifyRetention(Retention retention) {
    var retainUntilDate = retention.retainUntilDate();
    if (Instant.now().isAfter(retainUntilDate)) {
      throw INVALID_REQUEST_RETAIN_DATE;
    }
  }

  public void verifyMd5(Path input, @Nullable String contentMd5) {
    try {
      try (var stream = Files.newInputStream(input)) {
        verifyMd5(stream, contentMd5);
      }
    } catch (IOException e) {
      throw BAD_REQUEST_CONTENT;
    }
  }

  public void verifyMd5(InputStream inputStream, @Nullable String contentMd5) {
    if (contentMd5 != null) {
      var md5 = DigestUtil.base64Digest(inputStream);
      if (!md5.equals(contentMd5)) {
        LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5);
        throw BAD_REQUEST_MD5;
      }
    }
  }

  /**
   * For copy use-cases, we need to return PRECONDITION_FAILED only.
   */
  public void verifyObjectMatchingForCopy(
      @Nullable List<String> match,
      @Nullable List<String> noneMatch,
      @Nullable List<Instant> ifModifiedSince,
      @Nullable List<Instant> ifUnmodifiedSince,
      @Nullable S3ObjectMetadata s3ObjectMetadata) {
    try {
      verifyObjectMatching(match, noneMatch, ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);
    } catch (S3Exception e) {
      if (NOT_MODIFIED.equals(e)) {
        throw PRECONDITION_FAILED;
      } else {
        throw e;
      }
    }
  }

  public void verifyObjectMatching(
      String bucketName,
      String key,
      @Nullable List<String> match,
      @Nullable List<String> noneMatch) {
    try {
      var s3ObjectMetadataExisting = getObject(bucketName, key, null);
      verifyObjectMatching(match, noneMatch, null, null, s3ObjectMetadataExisting);
    } catch (S3Exception e) {
      if (e == NOT_MODIFIED) {
        throw PRECONDITION_FAILED;
      } else {
        throw e;
      }
    }
  }

  public void verifyObjectMatching(
      List<String> match,
      @Nullable List<Instant> matchLastModifiedTime,
      @Nullable List<Long> matchSize,
      @Nullable S3ObjectMetadata s3ObjectMetadata) {

    verifyObjectMatching(match, null, null, null, s3ObjectMetadata);
    if (s3ObjectMetadata != null) {
      if (matchLastModifiedTime != null && !matchLastModifiedTime.isEmpty()) {
        var lastModified = Instant.ofEpochMilli(s3ObjectMetadata.lastModified());
        if (!lastModified.truncatedTo(SECONDS).equals(matchLastModifiedTime.get(0).truncatedTo(SECONDS))) {
          throw PRECONDITION_FAILED;
        }
      }
      if (matchSize != null && !matchSize.isEmpty()) {
        var size = s3ObjectMetadata.size();
        if (!Long.valueOf(size).equals(matchSize.get(0))) {
          throw PRECONDITION_FAILED;
        }
      }
    }
  }

  public void verifyObjectMatching(
      @Nullable List<String> match,
      @Nullable List<String> noneMatch,
      @Nullable List<Instant> ifModifiedSince,
      @Nullable List<Instant> ifUnmodifiedSince,
      @Nullable S3ObjectMetadata s3ObjectMetadata) {
    if (s3ObjectMetadata == null) {
      // object does not exist,
      if (match != null && !match.isEmpty()) {
        // client expects an existing object to match a value, but it could not be found.
        throw NO_SUCH_KEY;
      }
      // no client expectations, skip the rest of the checks.
      return;
    }

    var etag = s3ObjectMetadata.etag();
    var lastModified = Instant.ofEpochMilli(s3ObjectMetadata.lastModified());

    var setModifiedSince = ifModifiedSince != null && !ifModifiedSince.isEmpty();
    if (setModifiedSince) {
      if (ifModifiedSince.get(0).isAfter(lastModified)) {
        LOG.debug("Object {} not modified since {}", s3ObjectMetadata.key(), ifModifiedSince.get(0));
        throw NOT_MODIFIED;
      }
    }

    var setMatch = match != null && !match.isEmpty();
    if (setMatch) {
      var unquotedEtag = etag.replace("\"", "");
      if (match.contains(WILDCARD_ETAG)
          || match.contains(WILDCARD)
          || match.contains(etag)
          || match.contains(unquotedEtag)
      ) {
        // request cares only that the object exists or that the etag matches.
        LOG.debug("Object {} exists", s3ObjectMetadata.key());
        return;
      } else if (!match.contains(unquotedEtag) && !match.contains(etag)) {
        LOG.debug("Object {} does not match etag {}", s3ObjectMetadata.key(), etag);
        throw PRECONDITION_FAILED;
      }
    }

    var setUnmodifiedSince = ifUnmodifiedSince != null && !ifUnmodifiedSince.isEmpty();
    if (setUnmodifiedSince) {
      if (ifUnmodifiedSince.get(0).isBefore(lastModified)) {
        LOG.debug("Object {} modified since {}", s3ObjectMetadata.key(), ifUnmodifiedSince.get(0));
        throw PRECONDITION_FAILED;
      }
    }

    var setNoneMatch = noneMatch != null && !noneMatch.isEmpty();
    if (setNoneMatch) {
      var unquotedEtag = etag.replace("\"", "");
      if (noneMatch.contains(WILDCARD_ETAG)
          || noneMatch.contains(WILDCARD)
          || noneMatch.contains(etag)
          || noneMatch.contains(unquotedEtag)
      ) {
        // request cares only that the object etag does not match.
        LOG.debug("Object {} has an ETag {} that matches one of the 'noneMatch' values", s3ObjectMetadata.key(), etag);
        throw NOT_MODIFIED;
      }
    }
  }

  public S3ObjectMetadata verifyObjectExists(String bucketName, String key, @Nullable String versionId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      throw NO_SUCH_KEY;
    }
    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucketMetadata, uuid, versionId);
    if (s3ObjectMetadata == null) {
      throw NO_SUCH_KEY;
    } else if (s3ObjectMetadata.deleteMarker()) {
      throw NO_SUCH_KEY_DELETE_MARKER;
    }
    return s3ObjectMetadata;
  }

  @Nullable
  public S3ObjectMetadata getObject(String bucketName, String key, @Nullable String versionId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }
    return objectStore.getS3ObjectMetadata(bucketMetadata, uuid, versionId);
  }

  public S3ObjectMetadata verifyObjectLockConfiguration(String bucketName, String key, @Nullable String versionId) {
    var s3ObjectMetadata = verifyObjectExists(bucketName, key, versionId);
    var noLegalHold = s3ObjectMetadata.legalHold() == null;
    var noRetention = s3ObjectMetadata.retention() == null;
    if (noLegalHold && noRetention) {
      throw NOT_FOUND_OBJECT_LOCK;
    }
    return s3ObjectMetadata;
  }
}
