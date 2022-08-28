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

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_MD5;
import static com.adobe.testing.s3mock.S3Exception.NOT_MODIFIED;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY;
import static com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;

import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.DeletedS3Object;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectService {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectService.class);
  private final BucketStore bucketStore;
  private final ObjectStore objectStore;

  public ObjectService(BucketStore bucketStore, ObjectStore objectStore) {
    this.bucketStore = bucketStore;
    this.objectStore = objectStore;
  }

  /**
   * Retrieves S3ObjectMetadata for a key from a bucket.
   *
   * @param bucket Bucket from which to retrieve the object.
   * @param key of the object.
   *
   * @return S3ObjectMetadata or null if not found
   */
  public S3ObjectMetadata getS3Object(String bucket, String key) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }

    return objectStore.getS3Object(bucketMetadata, uuid);
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucket bucket to copy from.
   * @param sourceKey object key to copy.
   * @param destinationBucket destination bucket.
   * @param destinationKey destination object key.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   * @param userMetadata User metadata to store for destination object
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   */
  public CopyObjectResult copyS3Object(String sourceBucket,
      String sourceKey,
      String destinationBucket,
      String destinationKey,
      String encryption,
      String kmsKeyId,
      Map<String, String> userMetadata) {
    BucketMetadata sourceBucketMetadata = bucketStore.getBucketMetadata(sourceBucket);
    BucketMetadata destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucket);
    UUID sourceId = sourceBucketMetadata.getID(sourceKey);
    if (sourceId == null) {
      return null;
    }

    // source and destination is the same, pretend we copied - S3 does the same.
    if (sourceKey.equals(destinationKey) && sourceBucket.equals(destinationBucket)) {
      return objectStore.pretendToCopyS3Object(sourceBucketMetadata, sourceId, userMetadata);
    }

    // source must be copied to destination
    UUID destinationId = bucketStore.addToBucket(destinationKey, destinationBucket);
    try {
      return objectStore.copyS3Object(sourceBucketMetadata, sourceId,
          destinationBucketMetadata, destinationId, destinationKey,
          encryption, kmsKeyId, userMetadata);
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket);
      throw e;
    }
  }

  /**
   * Stores an object inside a Bucket.
   *
   * @param bucket Bucket to store the object in.
   * @param key object key to be stored.
   * @param contentType The files Content Type.
   * @param contentEncoding The files Content Encoding.
   * @param dataStream The File as InputStream.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return {@link S3ObjectMetadata}.
   */
  public S3ObjectMetadata putS3Object(String bucket,
      String key,
      String contentType,
      String contentEncoding,
      InputStream dataStream,
      boolean useV4ChunkedWithSigningFormat,
      Map<String, String> userMetadata,
      String encryption,
      String kmsKeyId,
      List<Tag> tags) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID id = bucketMetadata.getID(key);
    if (id == null) {
      id = bucketStore.addToBucket(key, bucket);
    }
    return objectStore.putS3Object(bucketMetadata, id, key, contentType, contentEncoding,
        dataStream, useV4ChunkedWithSigningFormat, userMetadata, encryption, kmsKeyId, tags);
  }

  public DeleteResult deleteObjects(String bucket, Delete delete) {
    DeleteResult response = new DeleteResult();
    for (S3ObjectIdentifier object : delete.getObjectsToDelete()) {
      try {
        if (deleteObject(bucket, object.getKey())) {
          response.addDeletedObject(DeletedS3Object.from(object));
        } else {
          //TODO: There may be different error reasons than a non-existent key.
          response.addError(
              new com.adobe.testing.s3mock.dto.Error("NoSuchKey",
                  object.getKey(),
                  "The specified key does not exist.",
                  object.getVersionId()));
        }
      } catch (IllegalStateException e) {
        response.addError(
            new com.adobe.testing.s3mock.dto.Error("InternalError",
                object.getKey(),
                "We encountered an internal error. Please try again.",
                object.getVersionId()));
        LOG.error("Object could not be deleted!", e);
      }
    }
    return response;
  }

  /**
   * Removes an object key from a bucket.
   *
   * @param bucket bucket containing the object.
   * @param key object to be deleted.
   *
   * @return true if deletion succeeded.
   */
  public boolean deleteObject(String bucket, String key) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID id = bucketMetadata.getID(key);
    if (id == null) {
      return false;
    }

    if (objectStore.deleteObject(bucketMetadata, id)) {
      return bucketStore.removeFromBucket(key, bucket);
    } else {
      return false;
    }
  }

  /**
   * Sets tags for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param key object key to store tags for.
   * @param tags List of tag objects.
   */
  public void setObjectTags(String bucket, String key, List<Tag> tags) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketMetadata.getID(key);
    objectStore.setObjectTags(bucketMetadata, uuid, tags);
  }

  public InputStream verifyMd5(InputStream inputStream, String contentMd5,
      String sha256Header) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    copyTo(inputStream, byteArrayOutputStream);

    InputStream stream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    try {
      if (isV4ChunkedWithSigningEnabled(sha256Header)) {
        stream = new AwsChunkedDecodingInputStream(stream);
      }
      verifyMd5(stream, contentMd5);
    } finally {
      IOUtils.closeQuietly(stream);
    }
    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }

  public void verifyMd5(InputStream inputStream, String contentMd5) {
    if (contentMd5 != null) {
      String md5 = DigestUtil.base64Digest(inputStream);
      if (!md5.equals(contentMd5)) {
        LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5);
        throw BAD_REQUEST_MD5;
      }
    }
  }

  /**
   * Replace with InputStream.transferTo() once we update to Java 9+
   */
  private void copyTo(InputStream source, OutputStream target) {
    try {
      byte[] buf = new byte[8192];
      int length;
      while ((length = source.read(buf)) > 0) {
        target.write(buf, 0, length);
      }
    } catch (IOException e) {
      LOG.error("Could not copy streams.", e);
      throw new IllegalStateException("Could not copy streams.", e);
    }
  }

  public void verifyObjectMatching(List<String> match, List<String> noneMatch, String etag) {
    if (match != null && !match.contains(etag)) {
      throw PRECONDITION_FAILED;
    }
    if (noneMatch != null && noneMatch.contains(etag)) {
      throw NOT_MODIFIED;
    }
  }

  public S3ObjectMetadata verifyObjectExists(String bucket, String key) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      throw NO_SUCH_KEY;
    }
    S3ObjectMetadata s3ObjectMetadata = objectStore.getS3Object(bucketMetadata, uuid);
    if (s3ObjectMetadata == null) {
      throw NO_SUCH_KEY;
    }
    return s3ObjectMetadata;
  }
}
