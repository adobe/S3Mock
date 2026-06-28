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
 * Persisted per-part metadata written alongside each `<partNumber>.part` file.
 * Enables [MultipartStore.getMultipartUploadParts] to return checksums via ListParts,
 * and [MultipartStore.completeMultipartUpload] to populate [S3ObjectMetadata.parts] for
 * GetObjectAttributes.
 */
data class PartMetadata(
  val partNumber: Int,
  val etag: String?,
  val size: Long,
  val lastModified: Long,
  val checksum: String?,
  val checksumAlgorithm: ChecksumAlgorithm?,
)
