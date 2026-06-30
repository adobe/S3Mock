/*
 *  Copyright 2017-2026 Adobe.
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
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32C
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC64NVME
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA1
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256
import com.adobe.testing.s3mock.dto.serialization.EtagDeserializer
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonDeserialize

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html).
 */
@S3Verified(year = 2025)
data class CompletedPart(
  @param:JsonProperty("ChecksumCRC32", namespace = S3_NS)
  val checksumCRC32: String?,
  @param:JsonProperty("ChecksumCRC32C", namespace = S3_NS)
  val checksumCRC32C: String?,
  @param:JsonProperty("ChecksumCRC64NVME", namespace = S3_NS)
  val checksumCRC64NVME: String?,
  @param:JsonProperty("ChecksumSHA1", namespace = S3_NS)
  val checksumSHA1: String?,
  @param:JsonProperty("ChecksumSHA256", namespace = S3_NS)
  val checksumSHA256: String?,
  @param:JsonProperty("ETag", namespace = S3_NS)
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = S3_NS)
  val etag: String?,
  @param:JsonProperty("PartNumber", namespace = S3_NS)
  val partNumber: Int,
) {
  constructor(
    checksumAlgorithm: ChecksumAlgorithm?,
    checksum: String?,
    etag: String?,
    partNumber: Int,
  ) : this(
    checksumAlgorithm.ifAlgorithm(CRC32, checksum),
    checksumAlgorithm.ifAlgorithm(CRC32C, checksum),
    checksumAlgorithm.ifAlgorithm(CRC64NVME, checksum),
    checksumAlgorithm.ifAlgorithm(SHA1, checksum),
    checksumAlgorithm.ifAlgorithm(SHA256, checksum),
    etag,
    partNumber,
  )

  @JsonIgnore
  fun checksum(algorithm: ChecksumAlgorithm): String? =
    when (algorithm) {
      ChecksumAlgorithm.CRC32 -> checksumCRC32
      ChecksumAlgorithm.CRC32C -> checksumCRC32C
      ChecksumAlgorithm.CRC64NVME -> checksumCRC64NVME
      ChecksumAlgorithm.SHA1 -> checksumSHA1
      ChecksumAlgorithm.SHA256 -> checksumSHA256
    }
}
