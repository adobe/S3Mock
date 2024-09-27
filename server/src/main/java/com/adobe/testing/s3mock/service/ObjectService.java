/*
 *  Copyright 2017-2024 Adobe.
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
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_RETAINDATE;
import static com.adobe.testing.s3mock.S3Exception.NOT_FOUND_OBJECT_LOCK;
import static com.adobe.testing.s3mock.S3Exception.NOT_MODIFIED;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY;
import static com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Checksum;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectService extends ServiceBase {
  static final String WILDCARD_ETAG = "\"*\"";
  private static final Logger LOG = LoggerFactory.getLogger(ObjectService.class);
  private final BucketStore bucketStore;
  private final ObjectStore objectStore;

  public ObjectService(BucketStore bucketStore, ObjectStore objectStore) {
    this.bucketStore = bucketStore;
    this.objectStore = objectStore;
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucketName bucket to copy from.
   * @param sourceKey object key to copy.
   * @param destinationBucketName destination bucket.
   * @param destinationKey destination object key.
   * @param userMetadata User metadata to store for destination object
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   */
  public CopyObjectResult copyS3Object(String sourceBucketName,
      String sourceKey,
      String destinationBucketName,
      String destinationKey,
      Map<String, String> encryptionHeaders,
      Map<String, String> storeHeaders,
      Map<String, String> userMetadata,
      StorageClass storageClass) {
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
          encryptionHeaders,
          storeHeaders,
          userMetadata,
          storageClass);
    }

    // source must be copied to destination
    var destinationId = bucketStore.addKeyToBucket(destinationKey, destinationBucketName);
    try {
      return objectStore.copyS3Object(sourceBucketMetadata, sourceId,
          destinationBucketMetadata, destinationId, destinationKey,
          encryptionHeaders, storeHeaders, userMetadata, storageClass);
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucketName);
      throw e;
    }
  }

  /**
   * Stores an object inside a Bucket.
   *
   * @param bucketName Bucket to store the object in.
   * @param key object key to be stored.
   * @param contentType The files Content Type.
   * @param storeHeaders various headers to store
   * @param path The InputStream buffered in a file.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   *
   * @return {@link S3ObjectMetadata}.
   */
  public S3ObjectMetadata putS3Object(String bucketName,
      String key,
      String contentType,
      Map<String, String> storeHeaders,
      Path path,
      Map<String, String> userMetadata,
      Map<String, String> encryptionHeaders,
      List<Tag> tags,
      ChecksumAlgorithm checksumAlgorithm,
      String checksum,
      Owner owner,
      StorageClass storageClass) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      id = bucketStore.addKeyToBucket(key, bucketName);
    }
    return objectStore.storeS3ObjectMetadata(bucketMetadata, id, key, contentType, storeHeaders,
        path, userMetadata, encryptionHeaders, null, tags,
        checksumAlgorithm, checksum, owner, storageClass);
  }

  public DeleteResult deleteObjects(String bucketName, Delete delete) {
    var response = new DeleteResult(new ArrayList<>(), new ArrayList<>());
    for (var object : delete.objectsToDelete()) {
      try {
        // ignore result of delete object.
        deleteObject(bucketName, object.key());
        // add deleted object even if it does not exist S3 does the same.
        response.addDeletedObject(DeletedS3Object.from(object));
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

  /**
   * Removes an object key from a bucket.
   *
   * @param bucketName bucket containing the object.
   * @param key object to be deleted.
   *
   * @return true if deletion succeeded.
   */
  public boolean deleteObject(String bucketName, String key) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return false;
    }

    if (objectStore.deleteObject(bucketMetadata, id)) {
      return bucketStore.removeFromBucket(key, bucketName);
    } else {
      return false;
    }
  }

  /**
   * Sets tags for a given object.
   *
   * @param bucketName Bucket the object is stored in.
   * @param key object key to store tags for.
   * @param tags List of tagSet objects.
   */
  public void setObjectTags(String bucketName, String key, List<Tag> tags) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeObjectTags(bucketMetadata, uuid, tags);
  }

  /**
   * Sets LegalHold for a given object.
   *
   * @param bucketName Bucket the object is stored in.
   * @param key object key to store tags for.
   * @param legalHold the legal hold.
   */
  public void setLegalHold(String bucketName, String key, LegalHold legalHold) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeLegalHold(bucketMetadata, uuid, legalHold);
  }

  /**
   * Sets AccessControlPolicy for a given object.
   *
   * @param bucketName Bucket the object is stored in.
   * @param key object key to store tags for.
   * @param policy the ACL.
   */
  public void setAcl(String bucketName, String key, AccessControlPolicy policy) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeAcl(bucketMetadata, uuid, policy);
  }

  /**
   * Retrieves AccessControlPolicy for a given object.
   *
   * @param bucketName Bucket the object is stored in.
   * @param key object key to store tags for.
   */
  public AccessControlPolicy getAcl(String bucketName, String key) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    return objectStore.readAcl(bucketMetadata, uuid);
  }

  /**
   * Sets Retention for a given object.
   *
   * @param bucketName Bucket the object is stored in.
   * @param key object key to store tags for.
   * @param retention the retention.
   */
  public void setRetention(String bucketName, String key, Retention retention) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    objectStore.storeRetention(bucketMetadata, uuid, retention);
  }

  public void verifyRetention(Retention retention) {
    var retainUntilDate = retention.retainUntilDate();
    if (Instant.now().isAfter(retainUntilDate)) {
      throw INVALID_REQUEST_RETAINDATE;
    }
  }

  public void verifyMd5(Path input, String contentMd5) {
    try {
      try (var stream = Files.newInputStream(input)) {
        verifyMd5(stream, contentMd5);
      }
    } catch (IOException e) {
      throw BAD_REQUEST_CONTENT;
    }
  }

  public void verifyMd5(InputStream inputStream, String contentMd5) {
    if (contentMd5 != null) {
      var md5 = DigestUtil.base64Digest(inputStream);
      if (!md5.equals(contentMd5)) {
        LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5);
        throw BAD_REQUEST_MD5;
      }
    }
  }

  /**
   * FOr copy use-cases, we need to return PRECONDITION_FAILED only.
   */
  public void verifyObjectMatchingForCopy(List<String> match, List<String> noneMatch,
      S3ObjectMetadata s3ObjectMetadata) {
    try {
      verifyObjectMatching(match, noneMatch, s3ObjectMetadata);
    } catch (S3Exception e) {
      if (NOT_MODIFIED.equals(e)) {
        throw PRECONDITION_FAILED;
      } else {
        throw e;
      }
    }
  }

  public void verifyObjectMatching(List<String> match, List<String> noneMatch,
      S3ObjectMetadata s3ObjectMetadata) {
    if (s3ObjectMetadata != null) {
      var etag = s3ObjectMetadata.etag();
      if (match != null) {
        if (match.contains(WILDCARD_ETAG)) {
          //request cares only that the object exists
          return;
        } else if (!match.contains(etag)) {
          throw PRECONDITION_FAILED;
        }
      }
      if (noneMatch != null && (noneMatch.contains(WILDCARD_ETAG) || noneMatch.contains(etag))) {
        //request cares only that the object DOES NOT exist.
        throw NOT_MODIFIED;
      }
    }
  }

  public S3ObjectMetadata verifyObjectExists(String bucketName, String key) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      throw NO_SUCH_KEY;
    }
    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucketMetadata, uuid);
    if (s3ObjectMetadata == null) {
      throw NO_SUCH_KEY;
    }
    return s3ObjectMetadata;
  }

  public S3ObjectMetadata verifyObjectLockConfiguration(String bucketName, String key) {
    var s3ObjectMetadata = verifyObjectExists(bucketName, key);
    var noLegalHold = s3ObjectMetadata.legalHold() == null;
    var noRetention = s3ObjectMetadata.retention() == null;
    if (noLegalHold && noRetention) {
      throw NOT_FOUND_OBJECT_LOCK;
    }
    return s3ObjectMetadata;
  }

  public static Checksum getChecksum(S3ObjectMetadata s3ObjectMetadata) {
    ChecksumAlgorithm checksumAlgorithm = s3ObjectMetadata.checksumAlgorithm();
    if (checksumAlgorithm != null) {
      return new Checksum(
              checksumAlgorithm == ChecksumAlgorithm.CRC32 ? s3ObjectMetadata.checksum() : null,
              checksumAlgorithm == ChecksumAlgorithm.CRC32C ? s3ObjectMetadata.checksum() : null,
              checksumAlgorithm == ChecksumAlgorithm.SHA1 ? s3ObjectMetadata.checksum() : null,
              checksumAlgorithm == ChecksumAlgorithm.SHA256 ? s3ObjectMetadata.checksum() : null
      );
    }
    return null;
  }
}
