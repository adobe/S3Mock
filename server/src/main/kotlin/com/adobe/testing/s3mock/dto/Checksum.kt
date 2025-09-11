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
package com.adobe.testing.s3mock.dto

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html).
 */
@S3Verified(year = 2025)
data class Checksum(
  @field:JsonProperty("ChecksumCRC32")
  @param:JsonProperty("ChecksumCRC32")
  val checksumCRC32: String?,
  @field:JsonProperty("ChecksumCRC32C")
  @param:JsonProperty("ChecksumCRC32C")
  val checksumCRC32C: String?,
  @field:JsonProperty("ChecksumCRC64NVME")
  @param:JsonProperty("ChecksumCRC64NVME")
  val checksumCRC64NVME: String?,
  @field:JsonProperty("ChecksumSHA1")
  @param:JsonProperty("ChecksumSHA1")
  val checksumSHA1: String?,
  @field:JsonProperty("ChecksumSHA256")
  @param:JsonProperty("ChecksumSHA256")
  val checksumSHA256: String?,
  @field:JsonProperty("ChecksumType")
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?
) {
  companion object {
    fun from(s3ObjectMetadata: S3ObjectMetadata): Checksum? {
      val checksumAlgorithm = s3ObjectMetadata.checksumAlgorithm
      if (checksumAlgorithm != null) {
        return Checksum(
          if (checksumAlgorithm == ChecksumAlgorithm.CRC32) s3ObjectMetadata.checksum else null,
          if (checksumAlgorithm == ChecksumAlgorithm.CRC32C) s3ObjectMetadata.checksum else null,
          if (checksumAlgorithm == ChecksumAlgorithm.CRC64NVME) s3ObjectMetadata.checksum else null,
          if (checksumAlgorithm == ChecksumAlgorithm.SHA1) s3ObjectMetadata.checksum else null,
          if (checksumAlgorithm == ChecksumAlgorithm.SHA256) s3ObjectMetadata.checksum else null,
          s3ObjectMetadata.checksumType
        )
      }
      return null
    }
  }
}
