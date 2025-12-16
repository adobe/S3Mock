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
data class S3ObjectMetadata(
  val id: UUID,
  val key: String,
  val size: String,
  val modificationDate: String,
  val etag: String?,
  val contentType: String?,
  val lastModified: Long,
  val dataPath: Path,
  val userMetadata: Map<String, String>? = mapOf(),
  val tags: List<Tag>? = listOf(),
  val legalHold: LegalHold?,
  val retention: Retention?,
  val owner: Owner,
  val storeHeaders: Map<String, String>? = mapOf(),
  val encryptionHeaders: Map<String, String>? = mapOf(),
  val checksumAlgorithm: ChecksumAlgorithm?,
  val checksum: String?,
  val storageClass: StorageClass?,
  val policy: AccessControlPolicy?,
  val versionId: String?,
  val deleteMarker: Boolean = false,
  val checksumType: ChecksumType?
) {
  companion object {
    fun deleteMarker(metadata: S3ObjectMetadata, versionId: String?): S3ObjectMetadata =
      metadata.copy(deleteMarker = true, versionId = versionId)
  }
}
