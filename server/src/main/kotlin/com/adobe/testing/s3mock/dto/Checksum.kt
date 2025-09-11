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

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html">API Reference</a>.
 */
@S3Verified(year = 2025)
public record Checksum(
    @JsonProperty("ChecksumCRC32") String checksumCRC32,
    @JsonProperty("ChecksumCRC32C") String checksumCRC32C,
    @JsonProperty("ChecksumCRC64NVME") String checksumCRC64NVME,
    @JsonProperty("ChecksumSHA1") String checksumSHA1,
    @JsonProperty("ChecksumSHA256") String checksumSHA256,
    @JsonProperty("ChecksumType") ChecksumType checksumType
) {

  public static Checksum from(S3ObjectMetadata s3ObjectMetadata) {
    var checksumAlgorithm = s3ObjectMetadata.checksumAlgorithm;
    if (checksumAlgorithm != null) {
      return new Checksum(
          checksumAlgorithm == ChecksumAlgorithm.CRC32 ? s3ObjectMetadata.checksum : null,
          checksumAlgorithm == ChecksumAlgorithm.CRC32C ? s3ObjectMetadata.checksum : null,
          checksumAlgorithm == ChecksumAlgorithm.CRC64NVME ? s3ObjectMetadata.checksum : null,
          checksumAlgorithm == ChecksumAlgorithm.SHA1 ? s3ObjectMetadata.checksum : null,
          checksumAlgorithm == ChecksumAlgorithm.SHA256 ? s3ObjectMetadata.checksum : null,
          s3ObjectMetadata.checksumType
      );
    }
    return null;
  }
}
