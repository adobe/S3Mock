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

package com.adobe.testing.s3mock;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NOT_MODIFIED;
import static org.springframework.http.HttpStatus.PRECONDITION_FAILED;

import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class ControllerBase {
  private static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  protected static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  protected final FileStore fileStore;
  protected final BucketStore bucketStore;

  ControllerBase(FileStore fileStore, BucketStore bucketStore) {
    this.fileStore = fileStore;
    this.bucketStore = bucketStore;
  }

  protected void verifyObjectMatching(List<String> match, List<String> noneMatch, String etag) {
    if (match != null && !match.contains(etag)) {
      throw new S3Exception(PRECONDITION_FAILED.value(),
          "PreconditionFailed", "Precondition Failed");
    }
    if (noneMatch != null && noneMatch.contains(etag)) {
      throw new S3Exception(NOT_MODIFIED.value(), "NotModified", "Not Modified");
    }
  }

  protected S3ObjectMetadata verifyObjectExistence(String bucketName, String filename) {
    S3ObjectMetadata s3ObjectMetadata = fileStore.getS3Object(bucketName, filename);
    if (s3ObjectMetadata == null) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.");
    }
    return s3ObjectMetadata;
  }

  protected void verifyBucketExists(String bucketName) {
    if (!bucketStore.doesBucketExist(bucketName)) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchBucket",
          "The specified bucket does not exist.");
    }
  }

  protected void verifyBucketDoesNotExist(String bucketName) {
    if (bucketStore.doesBucketExist(bucketName)) {
      throw new S3Exception(CONFLICT.value(), "BucketAlreadyExists",
          "The requested bucket name is not available. "
              + "The bucket namespace is shared by all users of the system. "
              + "Please select a different name and try again.");
    }
  }

  protected void verifyPartNumberLimits(String partNumberString) {
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

  protected void validateMultipartParts(String bucketName, String filename,
      String uploadId, List<CompletedPart> requestedParts) throws S3Exception {
    validateMultipartParts(bucketName, filename, uploadId);

    List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    Map<Integer, String> uploadedPartsMap =
        uploadedParts
            .stream()
            .collect(Collectors.toMap(CompletedPart::getPartNumber, CompletedPart::getETag));

    Integer prevPartNumber = 0;
    for (CompletedPart part : requestedParts) {
      if (!uploadedPartsMap.containsKey(part.getPartNumber())
          || !uploadedPartsMap.get(part.getPartNumber())
          .equals(part.getETag().replaceAll("^\"|\"$", ""))) {
        throw new S3Exception(BAD_REQUEST.value(), "InvalidPart",
            "One or more of the specified parts could not be found. The part might not have been "
                + "uploaded, or the specified entity tag might not have matched the part's entity"
                + " tag.");
      }
      if (part.getPartNumber() < prevPartNumber) {
        throw new S3Exception(BAD_REQUEST.value(), "InvalidPartOrder",
            "The list of parts was not in ascending order. The parts list must be specified in "
                + "order by part number.");
      }
      prevPartNumber = part.getPartNumber();
    }
  }

  protected void validateMultipartParts(String bucketName, String filename,
      String uploadId) throws S3Exception {
    verifyMultipartUploadExists(uploadId);
    List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    if (uploadedParts.size() > 0) {
      for (int i = 0; i < uploadedParts.size() - 1; i++) {
        Part part = uploadedParts.get(i);
        if (part.getSize() < MINIMUM_PART_SIZE) {
          throw new S3Exception(BAD_REQUEST.value(), "EntityTooSmall",
              "Your proposed upload is smaller than the minimum allowed object size. "
                  + "Each part must be at least 5 MB in size, except the last part.");
        }
      }
    }
  }

  protected void verifyMultipartUploadExists(String uploadId) throws S3Exception {
    try {
      fileStore.getMultipartUpload(uploadId);
    } catch (IllegalArgumentException e) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchUpload",
          "The specified multipart upload does not exist. The upload ID might be invalid, or the "
              + "multipart upload might have been aborted or completed.");
    }
  }

}
