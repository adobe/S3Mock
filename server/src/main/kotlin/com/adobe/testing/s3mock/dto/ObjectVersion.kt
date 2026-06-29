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
import com.adobe.testing.s3mock.dto.EtagUtil.normalizeEtag
import com.adobe.testing.s3mock.dto.serialization.EtagDeserializer
import com.fasterxml.jackson.annotation.JsonProperty
import tools.jackson.databind.annotation.JsonDeserialize

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectVersion.html).
 */
@S3Verified(year = 2025)
data class ObjectVersion(
  @param:JsonProperty("ChecksumAlgorithm", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumAlgorithm: ChecksumAlgorithm?,
  @param:JsonProperty("ChecksumType", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val checksumType: ChecksumType?,
  @param:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  @param:JsonDeserialize(using = EtagDeserializer::class)
  @get:JsonProperty("ETag", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val etag: String?,
  @param:JsonProperty("IsLatest", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val isLatest: Boolean?,
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String?,
  @param:JsonProperty("LastModified", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val lastModified: String?,
  @param:JsonProperty("Owner", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val owner: Owner?,
  @param:JsonProperty("RestoreStatus", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val restoreStatus: RestoreStatus?,
  @param:JsonProperty("Size", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val size: String?,
  @param:JsonProperty("StorageClass", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val storageClass: StorageClass?,
  @param:JsonProperty("VersionId", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val versionId: String?,
) {
  companion object {
    /**
     * Use if versioning is not enabled.
     */
    fun from(s3Object: S3Object): ObjectVersion =
      ObjectVersion(
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
        "null",
      )
  }
}
