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
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteMarkerEntry.html).
 */
@S3Verified(year = 2025)
data class DeleteMarkerEntry(
  @param:JsonProperty("IsLatest", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val isLatest: Boolean?,
  @param:JsonProperty("Key", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val key: String?,
  @param:JsonProperty("LastModified", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val lastModified: String?,
  @param:JsonProperty("Owner", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val owner: Owner?,
  @param:JsonProperty("VersionId", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
  val versionId: String?
) {
  companion object {
    fun from(s3ObjectMetadata: S3ObjectMetadata, isLatest: Boolean): DeleteMarkerEntry {
      return DeleteMarkerEntry(
        isLatest,
        s3ObjectMetadata.key,
        s3ObjectMetadata.modificationDate,
        s3ObjectMetadata.owner,
        s3ObjectMetadata.versionId
      )
    }
  }
}
