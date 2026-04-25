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
 * Encapsulates metadata for a single part of a multipart upload.
 * Persisted as a JSON file alongside each `.part` binary file to enable checksum retrieval
 * when the [com.adobe.testing.s3mock.dto.CompleteMultipartUpload] request omits per-part checksums.
 */
data class PartMetadata(
  val partNumber: Int,
  val etag: String?,
  val checksum: String?,
  val checksumAlgorithm: ChecksumAlgorithm?,
  val size: Long,
  val lastModified: Long,
)
