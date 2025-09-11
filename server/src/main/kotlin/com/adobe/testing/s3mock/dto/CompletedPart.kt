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

package com.adobe.testing.s3mock.dto;

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.S3Verified;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record CompletedPart(
    @JsonProperty("ChecksumCRC32") String checksumCRC32,
    @JsonProperty("ChecksumCRC32C") String checksumCRC32C,
    @JsonProperty("ChecksumCRC64NVME") String checksumCRC64NVME,
    @JsonProperty("ChecksumSHA1") String checksumSHA1,
    @JsonProperty("ChecksumSHA256") String checksumSHA256,
    @JsonProperty("ETag") String etag,
    @JsonProperty("PartNumber") Integer partNumber
) {

  public CompletedPart {
    etag = normalizeEtag(etag);
  }

  public CompletedPart(
      ChecksumAlgorithm checksumAlgorithm,
      String checksum,
      String etag,
      Integer partNumber) {
    this(
        checksumAlgorithm == ChecksumAlgorithm.CRC32 ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.CRC32C ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.CRC64NVME ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.SHA1 ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.SHA256 ? checksum : null,
        etag,
        partNumber
    );
  }

  @JsonIgnore
  public String checksum(ChecksumAlgorithm algorithm) {
    return switch (algorithm) {
      case CRC32 -> checksumCRC32;
      case CRC32C -> checksumCRC32C;
      case CRC64NVME -> checksumCRC64NVME;
      case SHA1 -> checksumSHA1;
      case SHA256 -> checksumSHA256;
    };
  }
}
