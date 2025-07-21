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

package com.adobe.testing.s3mock.util

import com.fasterxml.jackson.annotation.JsonValue

/**
 * Headers used in HTTP requests from AWS S3 Client or in responses from S3.
 */
object AwsHttpHeaders {
    private const val NOT = "!"

    const val X_AMZ_BUCKET_REGION: String = "x-amz-bucket-region"
    const val X_AMZ_BUCKET_LOCATION_NAME: String = "x-amz-bucket-location-name"
    const val X_AMZ_BUCKET_LOCATION_TYPE: String = "x-amz-bucket-location-type"

    const val X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: String = "x-amz-server-side-encryption-aws-kms-key-id"

    const val X_AMZ_SERVER_SIDE_ENCRYPTION: String = "x-amz-server-side-encryption"
    const val NOT_X_AMZ_SERVER_SIDE_ENCRYPTION: String = NOT + X_AMZ_SERVER_SIDE_ENCRYPTION

    const val RANGE: String = "Range"

    const val X_AMZ_COPY_SOURCE: String = "x-amz-copy-source"
    const val NOT_X_AMZ_COPY_SOURCE: String = NOT + X_AMZ_COPY_SOURCE

    const val X_AMZ_COPY_SOURCE_RANGE: String = "x-amz-copy-source-range"
    const val NOT_X_AMZ_COPY_SOURCE_RANGE: String = NOT + X_AMZ_COPY_SOURCE_RANGE

    const val X_AMZ_COPY_SOURCE_IF_MATCH: String = "x-amz-copy-source-if-match"
    const val X_AMZ_COPY_SOURCE_IF_NONE_MATCH: String = "x-amz-copy-source-if-none-match"
    const val X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: String = "x-amz-copy-source-if-unmodified-since"
    const val X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: String = "x-amz-copy-source-if-modified-since"
    const val X_AMZ_COPY_SOURCE_VERSION_ID: String = "x-amz-copy-source-version-id"

    const val X_AMZ_METADATA_DIRECTIVE: String = "x-amz-metadata-directive"

    const val X_AMZ_CONTENT_SHA256: String = "x-amz-content-sha256"
    const val X_AMZ_TAGGING: String = "x-amz-tagging"
    const val CONTENT_MD5: String = "Content-MD5"

    const val X_AMZ_VERSION_ID: String = "x-amz-version-id"
    const val X_AMZ_DELETE_MARKER: String = "x-amz-delete-marker"

    const val X_AMZ_IF_MATCH_LAST_MODIFIED_TIME: String = "x-amz-if-match-last-modified-time"
    const val X_AMZ_IF_MATCH_SIZE: String = "x-amz-if-match-size"
    const val X_AMZ_OBJECT_SIZE: String = "x-amz-object-size"

    const val X_AMZ_BUCKET_OBJECT_LOCK_ENABLED: String = "x-amz-bucket-object-lock-enabled"
    const val X_AMZ_OBJECT_OWNERSHIP: String = "x-amz-object-ownership"
    const val X_AMZ_OBJECT_ATTRIBUTES: String = "x-amz-object-attributes"
    const val X_AMZ_CHECKSUM_ALGORITHM: String = "x-amz-checksum-algorithm"
    const val X_AMZ_SDK_CHECKSUM_ALGORITHM: String = "x-amz-sdk-checksum-algorithm"
    const val X_AMZ_CHECKSUM: String = "x-amz-checksum"
    const val X_AMZ_CHECKSUM_MODE: String = "x-amz-checksum-mode"
    const val X_AMZ_CHECKSUM_TYPE: String = "x-amz-checksum-type"
    const val X_AMZ_CHECKSUM_CRC32: String = "x-amz-checksum-crc32"
    const val X_AMZ_CHECKSUM_CRC32C: String = "x-amz-checksum-crc32c"
    const val X_AMZ_CHECKSUM_CRC64NVME: String = "x-amz-checksum-crc64nvme"
    const val X_AMZ_CHECKSUM_SHA1: String = "x-amz-checksum-sha1"
    const val X_AMZ_CHECKSUM_SHA256: String = "x-amz-checksum-sha256"
    const val X_AMZ_STORAGE_CLASS: String = "x-amz-storage-class"
    const val X_AMZ_ACL: String = "x-amz-acl"
    const val X_AMZ_DECODED_CONTENT_LENGTH: String = "x-amz-decoded-content-length"
    const val X_AMZ_TRAILER: String = "x-amz-trailer"
    const val AWS_CHUNKED: String = "aws-chunked"

    /**
     * This enum declares values of the optional "x-amz-metadata-directive" header.
     * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
     * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
     */
    enum class MetadataDirective(private val value: String) {
        COPY("COPY"),
        REPLACE("REPLACE");

        @JsonValue
        override fun toString(): String {
            return this.value
        }
    }
}
