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

import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
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
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods used by Controllers.
 * Errors thrown in verify methods are declared here:
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html">API Reference</a>
 */
abstract class ControllerBase {
  private static final Logger LOG = LoggerFactory.getLogger(ControllerBase.class);
  private static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;
  protected static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  protected final FileStore fileStore;
  protected final BucketStore bucketStore;

  ControllerBase(FileStore fileStore, BucketStore bucketStore) {
    this.fileStore = fileStore;
    this.bucketStore = bucketStore;
  }

  protected void verifyMaxKeys(Integer maxKeys) {
    if (maxKeys < 0) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "maxKeys should be non-negative");
    }
  }

  protected void verifyEncodingType(String encodingtype) {
    if (isNotEmpty(encodingtype) && !"url".equals(encodingtype)) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "encodingtype can only be none or 'url'");
    }
  }


  protected static InputStream verifyMd5(InputStream inputStream, String contentMd5,
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

  protected static void verifyMd5(InputStream inputStream, String contentMd5) {
    if (contentMd5 != null) {
      String md5 = DigestUtil.base64Digest(inputStream);
      if (!md5.equals(contentMd5)) {
        LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5);
        throw new S3Exception(BAD_REQUEST.value(), "BadRequest",
            "Content-MD5 does not match object md5");
      }
    }
  }

  /**
   * Replace with InputStream.transferTo() once we update to Java 9+
   */
  static void copyTo(InputStream source, OutputStream target) {
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

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">API Reference Bucket Naming</a>.
   */
  protected void verifyBucketNameIsAllowed(String bucketName) {
    if (!bucketName.matches("[a-z0-9.-]+")) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidBucketName",
          "The specified bucket is not valid.");
    }
  }

  protected void verifyBucketIsEmpty(String bucketName) {
    if (!bucketStore.isBucketEmpty(bucketName)) {
      throw new S3Exception(CONFLICT.value(), "BucketNotEmpty",
          "The bucket you tried to delete is not empty.");
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
