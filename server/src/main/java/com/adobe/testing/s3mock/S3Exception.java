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

package com.adobe.testing.s3mock;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.adobe.testing.s3mock.dto.ErrorResponse;
import org.springframework.http.HttpStatus;

/**
 * {@link RuntimeException} to communicate general S3 errors.
 * These are handled by ControllerConfiguration.S3MockExceptionHandler,
 * mapped to {@link ErrorResponse} and serialized.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">API Reference</a>
 */
public class S3Exception extends RuntimeException {
  private static final String INVALID_REQUEST_CODE = "InvalidRequest";
  private static final String BAD_REQUEST_CODE = "BadRequest";
  public static final S3Exception INVALID_PART_NUMBER =
      new S3Exception(BAD_REQUEST.value(), "InvalidArgument",
          "Part number must be an integer between 1 and 10000, inclusive");
  public static final S3Exception INVALID_PART = new S3Exception(BAD_REQUEST.value(), "InvalidPart",
      "One or more of the specified parts could not be found. The part might not have been "
          + "uploaded, or the specified entity tag may not match the part's entity tag.");
  public static final S3Exception INVALID_PART_ORDER =
      new S3Exception(BAD_REQUEST.value(), "InvalidPartOrder",
          "The list of parts was not in ascending order. The parts list must be specified in "
              + "order by part number.");

  public static final S3Exception INVALID_TAG =
      new S3Exception(BAD_REQUEST.value(), "InvalidTag",
          "Your request contains tag input that is not valid. For example, your request might contain "
              + "duplicate keys, keys or values that are too long, or system tags.");

  public static S3Exception completeRequestMissingChecksum(String algorithm, Integer partNumber) {
    return new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
        "The upload was created using a " + algorithm + " checksum. "
            + "The complete request must include the checksum for each part. "
            + "It was missing for part " + partNumber + " in the request.");
  }

  public static final S3Exception NO_SUCH_UPLOAD_MULTIPART =
      new S3Exception(NOT_FOUND.value(), "NoSuchUpload",
          "The specified multipart upload does not exist. The upload ID might be invalid, or the "
              + "multipart upload might have been aborted or completed.");
  public static final S3Exception ENTITY_TOO_SMALL =
      new S3Exception(BAD_REQUEST.value(), "EntityTooSmall",
          "Your proposed upload is smaller than the minimum allowed object size. "
              + "Each part must be at least 5 MB in size, except the last part.");
  public static final S3Exception NO_SUCH_BUCKET =
      new S3Exception(NOT_FOUND.value(), "NoSuchBucket",
          "The specified bucket does not exist.");
  public static final S3Exception NO_SUCH_LIFECYCLE_CONFIGURATION =
      new S3Exception(NOT_FOUND.value(), "NoSuchLifecycleConfiguration",
          "The lifecycle configuration does not exist.");
  public static final S3Exception NO_SUCH_KEY =
      new S3Exception(NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.");
  public static final S3Exception NO_SUCH_VERSION =
      new S3Exception(NOT_FOUND.value(), "NoSuchVersion", "The version ID specified in the "
          + "request does not match an existing version.");
  public static final S3Exception NO_SUCH_KEY_DELETE_MARKER =
      new S3Exception(NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.");
  public static final S3Exception NOT_MODIFIED =
      new S3Exception(HttpStatus.NOT_MODIFIED.value(), "NotModified", "Not Modified");
  public static final S3Exception PRECONDITION_FAILED =
      new S3Exception(HttpStatus.PRECONDITION_FAILED.value(),
          "PreconditionFailed", "At least one of the pre-conditions you specified did not hold");
  public static final S3Exception BUCKET_NOT_EMPTY =
      new S3Exception(CONFLICT.value(), "BucketNotEmpty",
          "The bucket you tried to delete is not empty.");
  public static final S3Exception INVALID_BUCKET_NAME =
      new S3Exception(BAD_REQUEST.value(), "InvalidBucketName",
          "The specified bucket is not valid.");
  public static final S3Exception BUCKET_ALREADY_EXISTS =
      new S3Exception(CONFLICT.value(), "BucketAlreadyExists",
          "The requested bucket name is not available. "
              + "The bucket namespace is shared by all users of the system. "
              + "Please select a different name and try again.");
  public static final S3Exception BUCKET_ALREADY_OWNED_BY_YOU =
      new S3Exception(CONFLICT.value(), "BucketAlreadyOwnedByYou",
          "Your previous request to create the named bucket succeeded and you already own it.");
  public static final S3Exception NOT_FOUND_BUCKET_OBJECT_LOCK =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "Bucket is missing Object Lock Configuration");
  public static final S3Exception NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "Bucket is missing Versioning Configuration");
  public static final S3Exception NOT_FOUND_OBJECT_LOCK =
      new S3Exception(NOT_FOUND.value(), "NotFound",
          "The specified object does not have a ObjectLock configuration");
  public static final S3Exception INVALID_REQUEST_RETAIN_DATE =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "The retain until date must be in the future!");
  public static final S3Exception INVALID_REQUEST_MAX_KEYS =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "maxKeys should be non-negative");
  public static final S3Exception INVALID_REQUEST_ENCODING_TYPE =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "encodingtype can only be none or 'url'");
  public static final S3Exception INVALID_COPY_REQUEST_SAME_KEY =
      new S3Exception(BAD_REQUEST.value(), INVALID_REQUEST_CODE,
          "This copy request is illegal because it is trying to copy an object to itself without "
              + "changing the object's metadata, storage class, website redirect location or "
              + "encryption attributes.");

  public static final S3Exception BAD_REQUEST_MD5 =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Content-MD5 does not match object md5");
  public static final S3Exception BAD_REQUEST_CONTENT =
      new S3Exception(BAD_REQUEST.value(), "UnexpectedContent",
          "This request contains unsupported content.");
  public static final S3Exception BAD_DIGEST =
      new S3Exception(BAD_REQUEST.value(), "BadDigest",
          "The Content-MD5 or checksum value that you specified did "
              + "not match what the server received.");
  public static final S3Exception BAD_CHECKSUM_SHA1 =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Value for x-amz-checksum-sha1 header is invalid.");
  public static final S3Exception BAD_CHECKSUM_SHA256 =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Value for x-amz-checksum-sha256 header is invalid.");
  public static final S3Exception BAD_CHECKSUM_CRC32 =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Value for x-amz-checksum-crc32 header is invalid.");
  public static final S3Exception BAD_CHECKSUM_CRC32C =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Value for x-amz-checksum-crc32c header is invalid.");
  public static final S3Exception BAD_CHECKSUM_CRC64NVME =
      new S3Exception(BAD_REQUEST.value(), BAD_REQUEST_CODE,
          "Value for x-amz-checksum-crc64nvme header is invalid.");

  private final int status;
  private final String code;
  private final String message;

  /**
   * Creates a new S3Exception to be mapped as an {@link ErrorResponse}.
   *
   * @param status The Error Status.
   * @param code The Error Code.
   * @param message The Error Message.
   */
  public S3Exception(final int status, final String code, final String message) {
    super(message);
    this.status = status;
    this.code = code;
    this.message = message;
  }

  public int getStatus() {
    return status;
  }

  public String getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    return message;
  }
}
