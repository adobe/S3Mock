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

import static com.adobe.testing.s3mock.S3Exception.ENTITY_TOO_SMALL;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_NUMBER;
import static com.adobe.testing.s3mock.S3Exception.INVALID_PART_ORDER;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_UPLOAD_MULTIPART;
import static software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes;

import com.adobe.testing.s3mock.S3Exception;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Prefix;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.MultipartStore;
import com.adobe.testing.s3mock.store.MultipartUploadInfo;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRange;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

public class MultipartService extends ServiceBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipartService.class);
  static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  private final BucketStore bucketStore;
  private final MultipartStore multipartStore;

  public MultipartService(BucketStore bucketStore, MultipartStore multipartStore) {
    this.bucketStore = bucketStore;
    this.multipartStore = multipartStore;
  }

  @Nullable
  public String putPart(
      String bucketName,
      String key,
      UUID uploadId,
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

  @Nullable
  public CopyPartResult copyPart(
      String bucketName,
      String key,
      HttpRange copyRange,
      String partNumber,
      String destinationBucket,
      String destinationKey,
      UUID uploadId,
      Map<String, String> encryptionHeaders,
      String versionId) {
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
              destinationBucketMetadata, destinationId, uploadId, encryptionHeaders, versionId);
      return CopyPartResult.from(new Date(), "\"" + partEtag + "\"");
    } catch (Exception e) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(destinationKey, destinationBucket);
      throw new IllegalStateException(String.format(
          "Could not copy part. sourceBucket=%s, destinationBucket=%s, key=%s, sourceId=%s, "
              + "destinationId=%s, uploadId=%s", sourceBucketMetadata, destinationBucketMetadata,
          key, sourceId, destinationId, uploadId
      ), e);
    }
  }

  @Nullable
  public ListPartsResult getMultipartUploadParts(
      String bucketName,
      String key,
      Integer maxParts,
      Integer partNumberMarker,
      UUID uploadId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return null;
    }
    var multipartUpload = multipartStore.getMultipartUpload(bucketMetadata, uploadId, false);
    var parts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId)
        .stream()
        .toList();

    parts = filterBy(parts, Part::partNumber, partNumberMarker);

    Integer nextPartNumberMarker = null;
    var isTruncated = false;
    if (parts.size() > maxParts) {
      parts = parts.subList(0, maxParts);
      nextPartNumberMarker = parts.get(maxParts - 1).partNumber();
      isTruncated = true;
    }

    return new ListPartsResult(
        bucketName,
        multipartUpload.checksumAlgorithm(),
        multipartUpload.checksumType(),
        multipartUpload.initiator(),
        isTruncated,
        key,
        maxParts,
        nextPartNumberMarker,
        multipartUpload.owner(),
        parts,
        partNumberMarker,
        multipartUpload.storageClass(),
        uploadId.toString(),
        null);
  }

  public void abortMultipartUpload(String bucketName, String key, UUID uploadId) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    try {
      multipartStore.abortMultipartUpload(bucketMetadata, id, uploadId);
    } finally {
      bucketStore.removeFromBucket(key, bucketName);
    }
  }

  @Nullable
  public CompleteMultipartUploadResult completeMultipartUpload(
      String bucketName,
      String key,
      UUID uploadId,
      List<CompletedPart> parts,
      Map<String, String> encryptionHeaders,
      String location,
      @Nullable String checksum,
      @Nullable ChecksumAlgorithm checksumAlgorithm) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
      return null;
    }
    var multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucketMetadata, uploadId);
    return multipartStore
        .completeMultipartUpload(bucketMetadata, key, id, uploadId, parts, encryptionHeaders,
            multipartUploadInfo, location, checksum, checksumAlgorithm);
  }

  public InitiateMultipartUploadResult createMultipartUpload(
      String bucketName,
      String key,
      @Nullable String contentType,
      Map<String, String> storeHeaders,
      Owner owner,
      Owner initiator,
      Map<String, String> userMetadata,
      Map<String, String> encryptionHeaders,
      List<Tag> tags,
      StorageClass storageClass,
      ChecksumType checksumType,
      ChecksumAlgorithm checksumAlgorithm) {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
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
          tags,
          storageClass,
          checksumType,
          checksumAlgorithm);
      return new InitiateMultipartUploadResult(bucketName, key, multipartUpload.uploadId());
    } catch (Exception e) {
      // something went wrong with writing the destination file, clean up ID from BucketStore.
      bucketStore.removeFromBucket(key, bucketName);
      throw new IllegalStateException(String.format(
          "Could prepare Multipart Upload. bucket=%s, key=%s, id=%s",
          bucketMetadata, key, id
      ), e);
    }
  }

  public ListMultipartUploadsResult listMultipartUploads(
      String bucketName,
      String delimiter,
      String encodingType,
      String keyMarker,
      Integer maxUploads,
      @Nullable String prefix,
      String uploadIdMarker
  ) {
    String nextKeyMarker = null;
    String nextUploadIdMarker = null;
    var isTruncated = false;
    var normalizedPrefix = prefix == null ? "" : prefix;

    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var contents = multipartStore
        .listMultipartUploads(bucketMetadata, prefix)
        .stream()
        .filter(mu -> mu.key().startsWith(normalizedPrefix))
        .sorted(Comparator.comparing(MultipartUpload::key))
        .toList();

    contents = filterBy(contents, MultipartUpload::key, keyMarker);

    var commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents, MultipartUpload::key);
    contents = filterBy(contents, MultipartUpload::key, commonPrefixes);
    if (maxUploads < contents.size()) {
      contents = contents.subList(0, maxUploads);
      isTruncated = true;
      if (maxUploads > 0) {
        nextKeyMarker = contents.get(maxUploads - 1).key();
        nextUploadIdMarker = contents.get(maxUploads - 1).uploadId();
      }
    }

    var returnDelimiter = delimiter;
    var returnKeyMarker = keyMarker;
    var returnPrefix = prefix;
    var returnCommonPrefixes = commonPrefixes;

    if (Objects.equals("url", encodingType)) {
      contents = mapContents(contents,
          object -> new MultipartUpload(
              object.checksumAlgorithm(),
              object.checksumType(),
              object.initiated(),
              object.initiator(),
              urlEncodeIgnoreSlashes(object.key()),
              object.owner(),
              object.storageClass(),
              object.uploadId()
          ));
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnCommonPrefixes = mapContents(commonPrefixes, SdkHttpUtils::urlEncodeIgnoreSlashes);
      returnDelimiter = urlEncodeIgnoreSlashes(delimiter);
      returnKeyMarker = urlEncodeIgnoreSlashes(keyMarker);
      nextKeyMarker = urlEncodeIgnoreSlashes(nextKeyMarker);
    }

    return new ListMultipartUploadsResult(bucketName,
        returnKeyMarker,
        returnDelimiter,
        returnPrefix,
        uploadIdMarker,
        maxUploads,
        isTruncated,
        nextKeyMarker,
        nextUploadIdMarker,
        contents,
        returnCommonPrefixes.stream().map(Prefix::new).toList(),
        encodingType
    );
  }

  public void verifyPartNumberLimits(String partNumberString) {
    try {
      var partNumber = Integer.parseInt(partNumberString);
      if (partNumber < 1 || partNumber > 10000) {
        LOG.error("Multipart part number invalid. partNumber={}", partNumberString);
        throw INVALID_PART_NUMBER;
      }
    } catch (NumberFormatException nfe) {
      LOG.error("Multipart part number invalid. partNumber={}", partNumberString, nfe);
      throw INVALID_PART_NUMBER;
    }
  }

  public void verifyMultipartParts(
      String bucketName,
      String key,
      UUID uploadId,
      List<CompletedPart> requestedParts
  ) throws S3Exception {
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var id = bucketMetadata.getID(key);
    if (id == null) {
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

  public void verifyMultipartParts(
      String bucketName,
      UUID id,
      UUID uploadId
  ) throws S3Exception {
    verifyMultipartUploadExists(bucketName, uploadId);
    var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
    var uploadedParts = multipartStore.getMultipartUploadParts(bucketMetadata, id, uploadId);
    if (!uploadedParts.isEmpty()) {
      for (int i = 0; i < uploadedParts.size() - 1; i++) {
        var part = uploadedParts.get(i);
        verifyPartNumberLimits(part.partNumber().toString());
        if (part.size() < MINIMUM_PART_SIZE) {
          LOG.error("Multipart part size too small. bucket={}, id={}, uploadId={}, size={}",
              bucketMetadata, id, uploadId, part.size());
          throw ENTITY_TOO_SMALL;
        }
      }
    }
  }

  public void verifyMultipartUploadExists(String bucketName, UUID uploadId) throws S3Exception {
    verifyMultipartUploadExists(bucketName, uploadId, false);
  }

  public @Nullable MultipartUploadInfo verifyMultipartUploadExists(
      String bucketName,
      UUID uploadId,
      boolean includeCompleted
  ) throws S3Exception {
    try {
      var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
      multipartStore.getMultipartUpload(bucketMetadata, uploadId, includeCompleted);
      return multipartStore.getMultipartUploadInfo(bucketMetadata, uploadId);
    } catch (IllegalArgumentException e) {
      throw NO_SUCH_UPLOAD_MULTIPART;
    }
  }
}
