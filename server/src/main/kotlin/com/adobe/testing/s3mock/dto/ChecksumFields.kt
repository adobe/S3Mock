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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC32C
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.CRC64NVME
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA1
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm.SHA256

/**
 * Routes a single checksum value to the correct per-algorithm field.
 *
 * DTOs that carry per-algorithm checksum fields (e.g. [Part], [ObjectPart]) have a flat
 * structure for correct XML serialization.  This value type encapsulates the dispatch logic
 * so that adding a new algorithm requires only a one-line change here rather than edits at
 * every construction site.
 */
data class ChecksumFields(
  val checksumCRC32: String?,
  val checksumCRC32C: String?,
  val checksumCRC64NVME: String?,
  val checksumSHA1: String?,
  val checksumSHA256: String?,
) {
  companion object {
    fun from(
      algorithm: ChecksumAlgorithm?,
      checksum: String?,
    ): ChecksumFields =
      ChecksumFields(
        checksumCRC32 = algorithm.ifAlgorithm(CRC32, checksum),
        checksumCRC32C = algorithm.ifAlgorithm(CRC32C, checksum),
        checksumCRC64NVME = algorithm.ifAlgorithm(CRC64NVME, checksum),
        checksumSHA1 = algorithm.ifAlgorithm(SHA1, checksum),
        checksumSHA256 = algorithm.ifAlgorithm(SHA256, checksum),
      )
  }
}
