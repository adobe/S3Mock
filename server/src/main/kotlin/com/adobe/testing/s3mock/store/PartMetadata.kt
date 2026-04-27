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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm

/**
 * Stores per-part checksum data for a multipart upload part.
 * Written alongside the binary part file during [MultipartStore.putPart] and used as a fallback
 * when the CompleteMultipartUpload request omits per-part checksums.
 */
data class PartMetadata(
  val checksumAlgorithm: ChecksumAlgorithm?,
  val checksum: String?,
)
