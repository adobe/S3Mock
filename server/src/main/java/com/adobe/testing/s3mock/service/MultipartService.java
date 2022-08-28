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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static org.springframework.http.HttpStatus.BAD_REQUEST;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.MultipartStore;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipartService {

  private static final Logger LOG = LoggerFactory.getLogger(MultipartService.class);
  private static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  private final BucketStore bucketStore;
  private final MultipartStore multipartStore;

  public MultipartService(BucketStore bucketStore, MultipartStore multipartStore) {
    this.bucketStore = bucketStore;
    this.multipartStore = multipartStore;
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
    UUID uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }
    return multipartStore.putPart(bucketMetadata, uuid, uploadId, partNumber, inputStream,
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
    UUID id = sourceBucketMetadata.getID(key);
    if (id == null) {
      return null;
    }
    // source must be copied to destination
    UUID destinationId = bucketStore.addToBucket(destinationKey, destinationBucket);
    try {
      String partEtag =
          multipartStore.copyPart(sourceBucketMetadata, id, copyRange, partNumber,
              destinationBucketMetadata, destinationId, uploadId);
      return CopyPartResult.from(new Date(), "\"" + partEtag + "\"");
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket);
      throw e;
    }
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
    UUID uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }
    List<Part> parts = multipartStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
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
    UUID uuid = bucketMetadata.getID(key);
    try {
      multipartStore.abortMultipartUpload(bucketMetadata, uuid, uploadId);
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
    UUID uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }

    String etag = multipartStore
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
      multipartStore.prepareMultipartUpload(bucketMetadata, key, uuid, contentType, contentEncoding,
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

    List<MultipartUpload> multipartUploads = multipartStore.listMultipartUploads(bucket, prefix);

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
    UUID uuid = bucketMetadata.getID(filename);
    if (uuid == null) {
      //TODO: is this the correct error?
      throw INVALID_PART;
    }
    verifyMultipartParts(bucketName, uuid, uploadId);

    List<Part> uploadedParts =
        multipartStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
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
        multipartStore.getMultipartUploadParts(bucketMetadata, uuid, uploadId);
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
      multipartStore.getMultipartUpload(uploadId);
    } catch (IllegalArgumentException e) {
      throw NO_SUCH_UPLOAD_MULTIPART;
    }
  }
}
