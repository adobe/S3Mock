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

package com.adobe.testing.s3mock.util;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Headers used in HTTP requests from AWS S3 Client or in responses from S3.
 */
public final class AwsHttpHeaders {

  private static final String NOT = "!";

  public static final String X_AMZ_BUCKET_REGION = "x-amz-bucket-region";
  public static final String X_AMZ_BUCKET_LOCATION_NAME = "x-amz-bucket-location-name";
  public static final String X_AMZ_BUCKET_LOCATION_TYPE = "x-amz-bucket-location-type";

  public static final String X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID =
      "x-amz-server-side-encryption-aws-kms-key-id";

  public static final String X_AMZ_SERVER_SIDE_ENCRYPTION = "x-amz-server-side-encryption";
  public static final String NOT_X_AMZ_SERVER_SIDE_ENCRYPTION = NOT + X_AMZ_SERVER_SIDE_ENCRYPTION;

  public static final String RANGE = "Range";

  public static final String X_AMZ_COPY_SOURCE = "x-amz-copy-source";
  public static final String NOT_X_AMZ_COPY_SOURCE = NOT + X_AMZ_COPY_SOURCE;

  public static final String X_AMZ_COPY_SOURCE_RANGE = "x-amz-copy-source-range";
  public static final String NOT_X_AMZ_COPY_SOURCE_RANGE = NOT + X_AMZ_COPY_SOURCE_RANGE;

  public static final String X_AMZ_COPY_SOURCE_IF_MATCH = "x-amz-copy-source-if-match";
  public static final String X_AMZ_COPY_SOURCE_IF_NONE_MATCH = "x-amz-copy-source-if-none-match";
  public static final String X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE =
      "x-amz-copy-source-if-unmodified-since";
  public static final String X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE =
      "x-amz-copy-source-if-modified-since";
  public static final String X_AMZ_COPY_SOURCE_VERSION_ID = "x-amz-copy-source-version-id";

  public static final String X_AMZ_METADATA_DIRECTIVE = "x-amz-metadata-directive";

  public static final String X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";
  public static final String X_AMZ_TAGGING = "x-amz-tagging";
  public static final String CONTENT_MD5 = "Content-MD5";

  public static final String X_AMZ_VERSION_ID = "x-amz-version-id";
  public static final String X_AMZ_DELETE_MARKER = "x-amz-delete-marker";

  public static final String X_AMZ_IF_MATCH_LAST_MODIFIED_TIME = "x-amz-if-match-last-modified-time";
  public static final String X_AMZ_IF_MATCH_SIZE = "x-amz-if-match-size";
  public static final String X_AMZ_OBJECT_SIZE = "x-amz-object-size";

  public static final String X_AMZ_BUCKET_OBJECT_LOCK_ENABLED = "x-amz-bucket-object-lock-enabled";
  public static final String X_AMZ_OBJECT_OWNERSHIP = "x-amz-object-ownership";
  public static final String X_AMZ_OBJECT_ATTRIBUTES = "x-amz-object-attributes";
  public static final String X_AMZ_CHECKSUM_ALGORITHM = "x-amz-checksum-algorithm";
  public static final String X_AMZ_SDK_CHECKSUM_ALGORITHM = "x-amz-sdk-checksum-algorithm";
  public static final String X_AMZ_CHECKSUM = "x-amz-checksum";
  public static final String X_AMZ_CHECKSUM_MODE = "x-amz-checksum-mode";
  public static final String X_AMZ_CHECKSUM_TYPE = "x-amz-checksum-type";
  public static final String X_AMZ_CHECKSUM_CRC32 = "x-amz-checksum-crc32";
  public static final String X_AMZ_CHECKSUM_CRC32C = "x-amz-checksum-crc32c";
  public static final String X_AMZ_CHECKSUM_CRC64NVME = "x-amz-checksum-crc64nvme";
  public static final String X_AMZ_CHECKSUM_SHA1 = "x-amz-checksum-sha1";
  public static final String X_AMZ_CHECKSUM_SHA256 = "x-amz-checksum-sha256";
  public static final String X_AMZ_STORAGE_CLASS = "x-amz-storage-class";
  public static final String X_AMZ_ACL = "x-amz-acl";
  public static final String X_AMZ_DECODED_CONTENT_LENGTH = "x-amz-decoded-content-length";
  public static final String X_AMZ_TRAILER = "x-amz-trailer";
  public static final String AWS_CHUNKED = "aws-chunked";

  private AwsHttpHeaders() {
    // private constructor for utility classes
  }

  /**
   * This enum declares values of the optional "x-amz-metadata-directive" header.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html">API Reference</a>
   */
  public enum MetadataDirective {

    COPY("COPY"),
    REPLACE("REPLACE");

    private final String value;

    MetadataDirective(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(this.value);
    }
  }
}
