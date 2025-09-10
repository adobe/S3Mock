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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import java.nio.file.Path
import java.util.UUID

/**
 * Represents an object in S3, used to serialize and deserialize all metadata locally.
 * [See API](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
 */
@JvmRecord
data class S3ObjectMetadata(
  @JvmField val id: UUID,
  @JvmField val key: String,
  @JvmField val size: String,
  @JvmField val modificationDate: String,
  @JvmField val etag: String?,
  @JvmField val contentType: String?,
  @JvmField val lastModified: Long,
  @JvmField val dataPath: Path,
  @JvmField val userMetadata: Map<String, String>? = mapOf(),
  @JvmField val tags: List<Tag>? = listOf(),
  @JvmField val legalHold: LegalHold?,
  @JvmField val retention: Retention?,
  @JvmField val owner: Owner,
  @JvmField val storeHeaders: Map<String, String>? = mapOf(),
  @JvmField val encryptionHeaders: Map<String, String>? = mapOf(),
  @JvmField val checksumAlgorithm: ChecksumAlgorithm?,
  @JvmField val checksum: String?,
  @JvmField val storageClass: StorageClass?,
  @JvmField val policy: AccessControlPolicy?,
  @JvmField val versionId: String?,
  @JvmField val deleteMarker: Boolean = false,
  @JvmField val checksumType: ChecksumType? = ChecksumType.FULL_OBJECT
) {
  companion object {
    fun deleteMarker(metadata: S3ObjectMetadata, versionId: String?): S3ObjectMetadata =
      metadata.copy(deleteMarker = true, versionId = versionId)
  }
}
