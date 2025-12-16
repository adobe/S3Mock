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
package com.adobe.testing.s3mock

import org.springframework.http.HttpStatus

/**
 * [RuntimeException] to communicate general S3 errors.
 * These are handled by ControllerConfiguration.S3MockExceptionHandler,
 * mapped to [ErrorResponse] and serialized.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
 */
class S3Exception
/**
 * Creates a new S3Exception to be mapped as an [ErrorResponse].
 *
 * @param status The Error Status.
 * @param code The Error Code.
 * @param message The Error Message.
 */
  (val status: Int, val code: String, override val message: String) : RuntimeException(message) {

    companion object {
        private const val INVALID_REQUEST_CODE = "InvalidRequest"
        private const val BAD_REQUEST_CODE = "BadRequest"
        private const val REQUESTED_RANGE_NOT_SATISFIABLE_CODE = "RequestedRangeNotSatisfiable"
        val INVALID_PART_NUMBER: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive"
        )
        val INVALID_PART: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "InvalidPart",
            "One or more of the specified parts could not be found. The part might not have been "
                    + "uploaded, or the specified entity tag may not match the part's entity tag."
        )
        val INVALID_PART_ORDER: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "InvalidPartOrder",
            "The list of parts was not in ascending order. The parts list must be specified in "
                    + "order by part number."
        )

        val INVALID_TAG: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "InvalidTag",
            "Your request contains tag input that is not valid. For example, your request might contain "
                    + "duplicate keys, keys or values that are too long, or system tags."
        )

        fun completeRequestMissingChecksum(algorithm: String, partNumber: Int): S3Exception {
            return S3Exception(
                HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
                ("The upload was created using a " + algorithm + " checksum. "
                        + "The complete request must include the checksum for each part. "
                        + "It was missing for part " + partNumber + " in the request.")
            )
        }

        fun completeRequestWrongChecksumMode(checksumMode: String): S3Exception {
            return S3Exception(
                HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
                ("The upload was created using the $checksumMode checksum mode. " +
                  "The complete request must use the same checksum mode.")
            )
        }

        val NO_SUCH_UPLOAD_MULTIPART: S3Exception = S3Exception(
            HttpStatus.NOT_FOUND.value(), "NoSuchUpload",
            "The specified multipart upload does not exist. The upload ID might be invalid, or the "
                    + "multipart upload might have been aborted or completed."
        )
        val ENTITY_TOO_SMALL: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "EntityTooSmall",
            "Your proposed upload is smaller than the minimum allowed object size. "
                    + "Each part must be at least 5 MB in size, except the last part."
        )
        val NO_SUCH_BUCKET: S3Exception = S3Exception(
            HttpStatus.NOT_FOUND.value(), "NoSuchBucket",
            "The specified bucket does not exist."
        )
        val NO_SUCH_LIFECYCLE_CONFIGURATION: S3Exception = S3Exception(
            HttpStatus.NOT_FOUND.value(), "NoSuchLifecycleConfiguration",
            "The lifecycle configuration does not exist."
        )
        val NO_SUCH_KEY: S3Exception =
            S3Exception(HttpStatus.NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.")
        val NO_SUCH_VERSION: S3Exception = S3Exception(
            HttpStatus.NOT_FOUND.value(), "NoSuchVersion", "The version ID specified in the "
                    + "request does not match an existing version."
        )
        val NO_SUCH_KEY_DELETE_MARKER: S3Exception =
            S3Exception(HttpStatus.NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.")
        val NOT_MODIFIED: S3Exception = S3Exception(HttpStatus.NOT_MODIFIED.value(), "NotModified", "Not Modified")
        val PRECONDITION_FAILED: S3Exception = S3Exception(
            HttpStatus.PRECONDITION_FAILED.value(),
            "PreconditionFailed", "At least one of the pre-conditions you specified did not hold"
        )
        val BUCKET_NOT_EMPTY: S3Exception = S3Exception(
            HttpStatus.CONFLICT.value(), "BucketNotEmpty",
            "The bucket you tried to delete is not empty."
        )
        val INVALID_BUCKET_NAME: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "InvalidBucketName",
            "The specified bucket is not valid."
        )
        val BUCKET_ALREADY_EXISTS: S3Exception = S3Exception(
            HttpStatus.CONFLICT.value(), "BucketAlreadyExists",
            ("The requested bucket name is not available. "
                    + "The bucket namespace is shared by all users of the system. "
                    + "Please select a different name and try again.")
        )
        val BUCKET_ALREADY_OWNED_BY_YOU: S3Exception = S3Exception(
            HttpStatus.CONFLICT.value(), "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded and you already own it."
        )
        val NOT_FOUND_BUCKET_OBJECT_LOCK: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            "Bucket is missing Object Lock Configuration"
        )
        val NOT_FOUND_BUCKET_VERSIONING_CONFIGURATION: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            "Bucket is missing Versioning Configuration"
        )
        val NOT_FOUND_OBJECT_LOCK: S3Exception = S3Exception(
            HttpStatus.NOT_FOUND.value(), "NotFound",
            "The specified object does not have a ObjectLock configuration"
        )
        val INVALID_REQUEST_RETAIN_DATE: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            "The retain until date must be in the future!"
        )
        val INVALID_REQUEST_MAX_KEYS: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            "maxKeys should be non-negative"
        )
        val INVALID_REQUEST_ENCODING_TYPE: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            "encodingtype can only be none or 'url'"
        )
        val INVALID_COPY_REQUEST_SAME_KEY: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), INVALID_REQUEST_CODE,
            ("This copy request is illegal because it is trying to copy an object to itself without "
                    + "changing the object's metadata, storage class, website redirect location or "
                    + "encryption attributes.")
        )

      val INVALID_RANGE: S3Exception = S3Exception(
        HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value(), REQUESTED_RANGE_NOT_SATISFIABLE_CODE,
      "Invalid http range.");

      val BAD_REQUEST_MD5: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Content-MD5 does not match object md5"
        )
        val BAD_REQUEST_CONTENT: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "UnexpectedContent",
            "This request contains unsupported content."
        )
        val BAD_DIGEST: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), "BadDigest",
            "The Content-MD5 or checksum value that you specified did "
                    + "not match what the server received."
        )
        val BAD_CHECKSUM_SHA1: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Value for x-amz-checksum-sha1 header is invalid."
        )
        val BAD_CHECKSUM_SHA256: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Value for x-amz-checksum-sha256 header is invalid."
        )
        val BAD_CHECKSUM_CRC32: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Value for x-amz-checksum-crc32 header is invalid."
        )
        val BAD_CHECKSUM_CRC32C: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Value for x-amz-checksum-crc32c header is invalid."
        )
        val BAD_CHECKSUM_CRC64NVME: S3Exception = S3Exception(
            HttpStatus.BAD_REQUEST.value(), BAD_REQUEST_CODE,
            "Value for x-amz-checksum-crc64nvme header is invalid."
        )
    }
}
