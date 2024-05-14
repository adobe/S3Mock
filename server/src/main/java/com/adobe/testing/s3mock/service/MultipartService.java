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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_NUMBER;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.MultipartStore;
import com.adobe.testing.s3mock.store.MultipartUploadInfo;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRange;

public class MultipartService extends ServiceBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipartService.class);
  static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  private final BucketStore bucketStore;
  private final MultipartStore multipartStore;

  public MultipartService(BucketStore bucketStore, MultipartStore multipartStore) {
    this.bucketStore = bucketStore;
    this.multipartStore = multipartStore;
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucketName                    in which to upload
   * @param key                      of the object to upload
   * @param uploadId                      id of the upload
   * @param partNumber                    number of the part to store
   * @param path                   file data to be stored
   *
   * @return the md5 digest of this part
   */
  public String putPart(String bucketName,
      String key,
      String uploadId,
      String partNumber,
      Path path,
      Map<String, String> encryptionHeaders) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uuid = bucketMetadata.getID(key);
    if (uuid == null) {
      return null;
    }
    return multipartStore.putPart(bucketMetadata, uuid, uploadId, partNumber,
        path, encryptionHeaders);
  }

  /**
   * Copies the range, define by from/to, from the S3 Object, identified by the given key to given
   * destination into the given bucket.
   *
   * @param bucketName The source Bucket.
   * @param key Identifies the S3 Object.
   * @param copyRange Byte range to copy. Optional.
   * @param partNumber The part to copy.
   * @param destinationBucket The Bucket the target object (will) reside in.
   * @param destinationKey The target object key.
   * @param uploadId id of the upload.
   *
   * @return etag of the uploaded file.
   */
  public CopyPartResult copyPart(String bucketName,
      String key,
      HttpRange copyRange,
      String partNumber,
      String destinationBucket,
      String destinationKey,
      String uploadId,
      Map<String, String> encryptionHeaders) {
    var sourceBucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var destinationBucketMetadata = bucketStore.getBucketMetadata(destinationBucket);
    var sourceId = sourceBucketMetadata.getID(key);
    if (sourceId == null) {
      return null;
    }
    // source must be copied to destination
    var destinationId = bucketStore.addKeyToBucket(destinationKey, destinationBucket);
    try {
      var partEtag =
          multipartStore.copyPart(sourceBucketMetadata, sourceId, copyRange, partNumber,
              destinationBucketMetadata, destinationId, uploadId, encryptionHeaders);
      return CopyPartResult.from(new Date(), "\"" + partEtag + "\"");
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket);
      throw new IllegalStateException(String.format(
          "Could not copy part. sourceBucket=%s, destinationBucket=%s, key=%s, sourceId=%s, "
              + "destinationId=%s, uploadId=%s", sourceBucketMetadata, destinationBucketMetadata,
          key, sourceId, destinationId, uploadId
      ), e);
    }
  }

  /**
   * Get all multipart upload parts.
   * @param bucketName name of the bucket
   * @param key object key
   * @param uploadId upload identifier
   * @return List of Parts
   */
  public ListPartsResult getMultipartUploadParts(String bucketName, String key, String uploadId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return null;
    }
    var parts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId);
    return new ListPartsResult(bucketName, key, uploadId, parts);
  }

  /**
   * Aborts the upload.
   *
   * @param bucketName to which was uploaded
   * @param key which was uploaded
   * @param uploadId of the upload
   */
  public void abortMultipartUpload(String bucketName, String key, String uploadId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    try {
      multipartStore.abortMultipartUpload(bucketMetadata, id, uploadId);
    } finally {
      bucketStore.removeFromBucket(key, bucketName);
    }
  }

  /**
   * Completes a Multipart Upload for the given ID.
   *
   * @param bucketName in which to upload.
   * @param key of the file to upload.
   * @param uploadId id of the upload.
   * @param parts to concatenate.
   * @param location the location link to embed in result
   *
   * @return etag of the uploaded file.
   */
  public Pair<CompleteMultipartUploadResult, MultipartUploadInfo> completeMultipartUpload(
      String bucketName,
      String key,
      String uploadId,
      List<CompletedPart> parts,
      Map<String, String> encryptionHeaders,
      String location) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return null;
    }
    var multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucketMetadata, uploadId);
    var etag = multipartStore
        .completeMultipartUpload(bucketMetadata, key, id, uploadId, parts, encryptionHeaders);
    return Pair.of(new CompleteMultipartUploadResult(location, bucketName, key, etag),
        multipartUploadInfo);
  }

  /**
   * Prepares everything to store an object uploaded as multipart upload.
   *
   * @param bucketName Bucket to upload object in
   * @param key object to upload
   * @param contentType the content type
   * @param storeHeaders various headers to store
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   * @param userMetadata custom metadata
   *
   * @return upload result
   */
  public InitiateMultipartUploadResult createMultipartUpload(String bucketName,
                                                             String key,
                                                             String contentType,
                                                             Map<String, String> storeHeaders,
                                                             Owner owner,
                                                             Owner initiator,
                                                             Map<String, String> userMetadata,
                                                             Map<String, String> encryptionHeaders,
                                                             StorageClass storageClass,
                                                             String checksum,
                                                             ChecksumAlgorithm checksumAlgorithm) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    //TODO: add upload to bucket
    var id = bucketStore.addKeyToBucket(key, bucketName);

    try {
      var multipartUpload = multipartStore.createMultipartUpload(bucketMetadata,
          key,
          id,
          contentType,
          storeHeaders,
          owner,
          initiator,
          userMetadata,
          encryptionHeaders,
          storageClass,
          checksum,
          checksumAlgorithm);
      return new InitiateMultipartUploadResult(bucketName, key, multipartUpload.uploadId());
    } catch (Exception e) {
      //something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(key, bucketName);
      throw new IllegalStateException(String.format(
          "Could prepare Multipart Upload. bucket=%s, key=%s, id=%s",
          bucketMetadata, key, id
      ), e);
    }
  }

  /**
   * Lists all not-yet completed parts of multipart uploads in a bucket.
   *
   * @param bucketName the bucket to use as a filter
   * @param prefix the prefix use as a filter
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public ListMultipartUploadsResult listMultipartUploads(String bucketName, String prefix) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var multipartUploads =
        multipartStore.listMultipartUploads(bucketMetadata, prefix);

    // the result contains all uploads, use some common value as default
    var maxUploads = Math.max(1000, multipartUploads.size());
    // delimiter / prefix search not supported
    return new ListMultipartUploadsResult(bucketName, null, null, prefix, null,
        maxUploads, false, null, null, multipartUploads,
        Collections.emptyList());
  }

  public void verifyPartNumberLimits(String partNumberString) {
    int partNumber;
    try {
      partNumber = Integer.parseInt(partNumberString);
      if (partNumber < 1 || partNumber > 10000) {
        LOG.error("Multipart part number invalid. partNumber={}", partNumberString);
        throw INVALID_PART_NUMBER;
      }
    } catch (NumberFormatException nfe) {
      LOG.error("Multipart part number invalid. partNumber={}", partNumberString, nfe);
      throw INVALID_PART_NUMBER;
    }
  }

  public void verifyMultipartParts(String bucketName, String key,
      String uploadId, List<CompletedPart> requestedParts) throws S3Exception {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      //TODO: is this the correct error?
      throw INVALID_PART;
    }
    verifyMultipartParts(bucketName, id, uploadId);

    var uploadedParts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId);
    var uploadedPartsMap =
        uploadedParts
            .stream()
            .collect(Collectors.toMap(Part::partNumber, Part::etag));

    var prevPartNumber = 0;
    for (var part : requestedParts) {
      if (!uploadedPartsMap.containsKey(part.partNumber())
          || !uploadedPartsMap.get(part.partNumber()).equals(part.etag())) {
        LOG.error("Multipart part not valid. bucket={}, id={}, uploadId={}, partNumber={}",
            bucketMetadata, id, uploadId, part.partNumber());
        throw INVALID_PART;
      }
      if (part.partNumber() < prevPartNumber) {
        LOG.error("Multipart parts order invalid. bucket={}, id={}, uploadId={}, partNumber={}",
            bucketMetadata, id, uploadId, part.partNumber());
        throw INVALID_PART_ORDER;
      }
      prevPartNumber = part.partNumber();
    }
  }

  public void verifyMultipartParts(String bucketName, UUID id, String uploadId) throws S3Exception {
    verifyMultipartUploadExists(bucketName, uploadId);
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uploadedParts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId);
    if (!uploadedParts.isEmpty()) {
      for (int i = 0; i < uploadedParts.size() - 1; i++) {
        var part = uploadedParts.get(i);
        if (part.size() < MINIMUM_PART_SIZE) {
          LOG.error("Multipart part size too small. bucket={}, id={}, uploadId={}, size={}",
              bucketMetadata, id, uploadId, part.size());
          throw ENTITY_TOO_SMALL;
        }
      }
    }
  }

  public void verifyMultipartUploadExists(String bucketName,
                                          String uploadId) throws S3Exception {
    try {
      var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
      multipartStore.getMultipartUpload(bucketMetadata, uploadId);
    } catch (IllegalArgumentException e) {
      throw NO_SUCH_UPLOAD_MULTIPART;
    }
  }
}
