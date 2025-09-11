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
import com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectVersion.html).
 */
@S3Verified(year = 2025)
class ObjectVersion(
  @field:JsonProperty("ChecksumAlgorithm")
  @param:JsonProperty("ChecksumAlgorithm")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @field:JsonProperty("ChecksumType")
  @param:JsonProperty("ChecksumType")
  val checksumType: ChecksumType?,
  @JsonProperty("ETag")
  etag: String?,
  @field:JsonProperty("IsLatest")
  @param:JsonProperty("IsLatest")
  val isLatest: Boolean?,
  @field:JsonProperty("Key")
  @param:JsonProperty("Key")
  val key: String?,
  @field:JsonProperty("LastModified")
  @param:JsonProperty("LastModified")
  val lastModified: String?,
  @field:JsonProperty("Owner")
  @param:JsonProperty("Owner")
  val owner: Owner?,
  @field:JsonProperty("RestoreStatus")
  @param:JsonProperty("RestoreStatus")
  val restoreStatus: RestoreStatus?,
  @field:JsonProperty("Size")
  @param:JsonProperty("Size")
  val size: String?,
  @field:JsonProperty("StorageClass")
  @param:JsonProperty("StorageClass")
  val storageClass: StorageClass?,
  @field:JsonProperty("VersionId")
  @param:JsonProperty("VersionId")
  val versionId: String?
) {
  @JsonProperty("ETag")
  val etag: String?

  init {
    var etag = etag
    etag = normalizeEtag(etag)
    this.etag = etag
  }

  companion object {
    fun from(s3ObjectMetadata: S3ObjectMetadata, isLatest: Boolean): ObjectVersion {
      return ObjectVersion(
        s3ObjectMetadata.checksumAlgorithm,
        s3ObjectMetadata.checksumType,
        normalizeEtag(s3ObjectMetadata.etag),
        isLatest,
        s3ObjectMetadata.key,
        s3ObjectMetadata.modificationDate,
        s3ObjectMetadata.owner,
        null,
        s3ObjectMetadata.size,
        s3ObjectMetadata.storageClass,
        s3ObjectMetadata.versionId
      )
    }

    /**
     * Use if versioning is not enabled.
     */
    fun from(s3Object: S3Object): ObjectVersion {
      return ObjectVersion(
        s3Object.checksumAlgorithm,
        s3Object.checksumType,
        normalizeEtag(s3Object.etag),
        true,
        s3Object.key,
        s3Object.lastModified,
        s3Object.owner,
        s3Object.restoreStatus,
        s3Object.size,
        s3Object.storageClass,
        "null"
      )
    }
  }
}
