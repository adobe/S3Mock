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
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html).
 */
@S3Verified(year = 2025)
class CompletedPart(
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
  @JsonProperty("ETag") etag: String?,
  @field:JsonProperty("PartNumber")
  @param:JsonProperty("PartNumber")
  val partNumber: Int
) {
  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  constructor(
    checksumAlgorithm: ChecksumAlgorithm?,
    checksum: String?,
    etag: String?,
    partNumber: Int
  ) : this(
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC32C) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.CRC64NVME) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA1) checksum else null,
    if (checksumAlgorithm == ChecksumAlgorithm.SHA256) checksum else null,
    etag,
    partNumber
  )

  @JsonIgnore
  fun checksum(algorithm: ChecksumAlgorithm): String? {
    return when (algorithm) {
      ChecksumAlgorithm.CRC32 -> checksumCRC32
      ChecksumAlgorithm.CRC32C -> checksumCRC32C
      ChecksumAlgorithm.CRC64NVME -> checksumCRC64NVME
      ChecksumAlgorithm.SHA1 -> checksumSHA1
      ChecksumAlgorithm.SHA256 -> checksumSHA256
    }
  }
}
