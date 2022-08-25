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
import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NOT_MODIFIED;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static com.adobe.testing.s3mock.S3Exception.PRECONDITION_FAILED;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static org.springframework.http.HttpStatus.BAD_REQUEST;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.DeletedS3Object;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectService {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectService.class);
  private static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  private final BucketStore bucketStore;
  private final FileStore fileStore;

  public ObjectService(BucketStore bucketStore, FileStore fileStore) {
    this.bucketStore = bucketStore;
    this.fileStore = fileStore;
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
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    if (uuid == null) {
      return null;
    }

    return fileStore.getS3Object(bucketMetadata, uuid);
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
    UUID sourceId = bucketStore.lookupKeyInBucket(sourceKey, sourceBucket);
    if (sourceId == null) {
      return null;
    }

    // source and destination is the same, pretend we copied - S3 does the same.
    if (sourceKey.equals(destinationKey) && sourceBucket.equals(destinationBucket)) {
      return fileStore.pretendToCopyS3Object(sourceBucketMetadata, sourceId, userMetadata);
    }

    // source must be copied to destination
    UUID destinationId = bucketStore.addToBucket(destinationKey, destinationBucket);
    try {
      return fileStore.copyS3Object(sourceBucketMetadata, sourceId,
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
    UUID id = bucketStore.lookupKeyInBucket(key, bucket);
    if (id == null) {
      id = bucketStore.addToBucket(key, bucket);
    }
    return fileStore.putS3Object(bucketMetadata, id, key, contentType, contentEncoding, dataStream,
        useV4ChunkedWithSigningFormat, userMetadata, encryption, kmsKeyId, tags);
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucket                    in which to upload
   * @param key                      of the object to upload
   * @param uploadId                      id of the upload
   * @param partNumber                    number of the part to store
   * @param inputStream                   file data to be stored
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param encryption                    whether to use encryption, and possibly which type
   * @param kmsKeyId                      the ID of the KMS key to use.
   *
   * @return the md5 digest of this part
   */
  public String putPart(String bucket,
      String key,
      String uploadId,
      String partNumber,
      InputStream inputStream,
      boolean useV4ChunkedWithSigningFormat,
      String encryption,
      String kmsKeyId) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    if (uuid == null) {
      return null;
    }
    return fileStore.putPart(bucketMetadata, uuid, uploadId, partNumber, inputStream,
        useV4ChunkedWithSigningFormat, encryption, kmsKeyId);
  }

  /**
   * Copies the range, define by from/to, from the S3 Object, identified by the given key to given
   * destination into the given bucket.
   *
   * @param bucket The source Bucket.
   * @param key Identifies the S3 Object.
   * @param copyRange Byte range to copy. Optional.
   * @param partNumber The part to copy.
   * @param destinationBucket The Bucket the target object (will) reside in.
   * @param destinationKey The target object key.
   * @param uploadId id of the upload.
   *
   * @return etag of the uploaded file.
   */
  public CopyPartResult copyPart(String bucket,
      String key,
      Range copyRange,
      String partNumber,
      String destinationBucket,
      String destinationKey,
      String uploadId) {
    BucketMetadata sourceBucketMetadata = bucketStore.getBucketMetadata(bucket);
    BucketMetadata destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucket);
    UUID id = bucketStore.lookupKeyInBucket(key, bucket);
    if (id == null) {
      return null;
    }
    // source must be copied to destination
    UUID destinationId = bucketStore.addToBucket(destinationKey, destinationBucket);
    try {
      String partEtag =
          fileStore.copyPart(sourceBucketMetadata, id, copyRange, partNumber,
              destinationBucketMetadata, destinationId, uploadId);
      return CopyPartResult.from(new Date(), "\"" + partEtag + "\"");
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket);
      throw e;
    }
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
    UUID id = bucketStore.lookupKeyInBucket(key, bucket);
    if (id == null) {
      return false;
    }

    if (fileStore.deleteObject(bucketMetadata, id)) {
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
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    fileStore.setObjectTags(bucketMetadata, uuid, tags);
  }

  /**
   * Get all multipart upload parts.
   * @param bucket name of the bucket
   * @param key object key
   * @param uploadId upload identifier
   * @return List of Parts
   */
  public ListPartsResult getMultipartUploadParts(String bucket, String key, String uploadId) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    if (uuid == null) {
      return null;
    }
    List<Part> parts = fileStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
    return new ListPartsResult(bucket, key, uploadId, parts);
  }

  /**
   * Aborts the upload.
   *
   * @param bucket to which was uploaded
   * @param key which was uploaded
   * @param uploadId of the upload
   */
  public void abortMultipartUpload(String bucket, String key, String uploadId) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    try {
      fileStore.abortMultipartUpload(bucketMetadata, uuid, uploadId);
    } finally {
      bucketStore.removeFromBucket(key, bucket);
    }
  }

  /**
   * Completes a Multipart Upload for the given ID.
   *
   * @param bucket in which to upload.
   * @param key of the file to upload.
   * @param uploadId id of the upload.
   * @param parts to concatenate.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return etag of the uploaded file.
   */
  public CompleteMultipartUploadResult completeMultipartUpload(String bucket, String key,
      String uploadId, List<CompletedPart> parts, String encryption, String kmsKeyId,
      String location) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    if (uuid == null) {
      return null;
    }

    String etag = fileStore
        .completeMultipartUpload(bucketMetadata, key, uuid, uploadId, parts, encryption, kmsKeyId);
    return new CompleteMultipartUploadResult(location, bucket, key, etag);
  }

  /**
   * Prepares everything to store an object uploaded as multipart upload.
   *
   * @param bucket Bucket to upload object in
   * @param key object to upload
   * @param contentType the content type
   * @param contentEncoding the content encoding
   * @param uploadId id of the upload
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   * @param userMetadata custom metadata
   *
   * @return upload result
   */
  public InitiateMultipartUploadResult prepareMultipartUpload(String bucket, String key,
      String contentType, String contentEncoding, String uploadId,
      Owner owner, Owner initiator, Map<String, String> userMetadata) {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucket);
    UUID uuid = bucketStore.addToBucket(key, bucket);

    try {
      fileStore.prepareMultipartUpload(bucketMetadata, key, uuid, contentType, contentEncoding,
          uploadId, owner, initiator, userMetadata);
      return new InitiateMultipartUploadResult(bucket, key, uploadId);
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(key, bucket);
      throw e;
    }
  }

  /**
   * Lists all not-yet completed parts of multipart uploads in a bucket.
   *
   * @param bucket the bucket to use as a filter
   * @param prefix the prefix use as a filter
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public ListMultipartUploadsResult listMultipartUploads(String bucket, String prefix) {

    List<MultipartUpload> multipartUploads = fileStore.listMultipartUploads(bucket, prefix);

    // the result contains all uploads, use some common value as default
    int maxUploads = Math.max(1000, multipartUploads.size());
    boolean isTruncated = false;
    String uploadIdMarker = null;
    String nextUploadIdMarker = null;
    String keyMarker = null;
    String nextKeyMarker = null;

    // delimiter / prefix search not supported
    String delimiter = null;
    List<String> commonPrefixes = Collections.emptyList();

    return new ListMultipartUploadsResult(bucket, keyMarker, delimiter, prefix, uploadIdMarker,
        maxUploads, isTruncated, nextKeyMarker, nextUploadIdMarker, multipartUploads,
        commonPrefixes);
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
    UUID uuid = bucketStore.lookupKeyInBucket(key, bucket);
    if (uuid == null) {
      throw NO_SUCH_KEY;
    }
    S3ObjectMetadata s3ObjectMetadata = fileStore.getS3Object(bucketMetadata, uuid);
    if (s3ObjectMetadata == null) {
      throw NO_SUCH_KEY;
    }
    return s3ObjectMetadata;
  }

  public void verifyPartNumberLimits(String partNumberString) {
    int partNumber;
    try {
      partNumber = Integer.parseInt(partNumberString);
    } catch (NumberFormatException nfe) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
    if (partNumber < 1 || partNumber > 10000) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
  }

  public void verifyMultipartParts(String bucketName, String filename,
      String uploadId, List<CompletedPart> requestedParts) throws S3Exception {
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    UUID uuid = bucketStore.lookupKeyInBucket(filename, bucketName);
    if (uuid == null) {
      //TODO: is this the correct error?
      throw INVALID_PART;
    }
    verifyMultipartParts(bucketName, uuid, uploadId);

    List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
    Map<Integer, String> uploadedPartsMap =
        uploadedParts
            .stream()
            .collect(Collectors.toMap(CompletedPart::getPartNumber, CompletedPart::getETag));

    Integer prevPartNumber = 0;
    for (CompletedPart part : requestedParts) {
      if (!uploadedPartsMap.containsKey(part.getPartNumber())
          || !uploadedPartsMap.get(part.getPartNumber())
          .equals(part.getETag().replaceAll("^\"|\"$", ""))) {
        throw INVALID_PART;
      }
      if (part.getPartNumber() < prevPartNumber) {
        throw INVALID_PART_ORDER;
      }
      prevPartNumber = part.getPartNumber();
    }
  }

  public void verifyMultipartParts(String bucketName, UUID uuid,
      String uploadId) throws S3Exception {
    verifyMultipartUploadExists(uploadId);
    BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
    if (uploadedParts.size() > 0) {
      for (int i = 0; i < uploadedParts.size() - 1; i++) {
        Part part = uploadedParts.get(i);
        if (part.getSize() < MINIMUM_PART_SIZE) {
          throw ENTITY_TOO_SMALL;
        }
      }
    }
  }

  public void verifyMultipartUploadExists(String uploadId) throws S3Exception {
    try {
      fileStore.getMultipartUpload(uploadId);
    } catch (IllegalArgumentException e) {
      throw NO_SUCH_UPLOAD_MULTIPART;
    }
  }
}
