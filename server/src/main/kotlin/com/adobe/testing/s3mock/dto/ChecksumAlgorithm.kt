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
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm

@S3Verified(year = 2025)
enum class ChecksumAlgorithm @JsonCreator constructor(private val value: String) {
  CRC32("CRC32"),
  CRC32C("CRC32C"),
  CRC64NVME("CRC64NVME"),
  SHA1("SHA1"),
  SHA256("SHA256");

  fun toChecksumAlgorithm(): software.amazon.awssdk.checksums.spi.ChecksumAlgorithm {
    return when (this) {
      CRC32 -> DefaultChecksumAlgorithm.CRC32
      CRC32C -> DefaultChecksumAlgorithm.CRC32C
      CRC64NVME -> DefaultChecksumAlgorithm.CRC64NVME
      SHA1 -> DefaultChecksumAlgorithm.SHA1
      SHA256 -> DefaultChecksumAlgorithm.SHA256
    }
  }

  @JsonValue
  override fun toString(): String {
    return this.value
  }

  companion object {
    fun fromString(value: String?): ChecksumAlgorithm? {
      return when (value) {
        "sha256", "SHA256" -> SHA256
        "sha1", "SHA1" -> SHA1
        "crc32", "CRC32" -> CRC32
        "crc32c", "CRC32C" -> CRC32C
        "crc64nvme", "CRC64NVME" -> CRC64NVME
        else -> null
      }
    }

    fun fromHeader(value: String?): ChecksumAlgorithm? {
      return when (value) {
        "x-amz-checksum-sha256" -> SHA256
        "x-amz-checksum-sha1" -> SHA1
        "x-amz-checksum-crc32" -> CRC32
        "x-amz-checksum-crc32c" -> CRC32C
        "x-amz-checksum-crc64nvme" -> CRC64NVME
        else -> null
      }
    }
  }
}
